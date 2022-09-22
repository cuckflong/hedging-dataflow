from ctrader_open_api import Client, EndPoints, Protobuf, TcpProtocol
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAAccountAuthRes,
    ProtoOAApplicationAuthReq,
    ProtoOAApplicationAuthRes,
    ProtoOAReconcileReq,
    ProtoOAReconcileRes,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from prefect import flow, get_run_logger
from prefect.blocks.system import Secret, String
from twisted.internet import reactor


@flow(name="cTrader Aggregate Positions Flow")
def ctrader_get_positions_data():
    logger = get_run_logger()

    hostType = "live"

    account_id = int(Secret.load("ctrader-account-id").get())
    client_id = Secret.load("ctrader-client-id").get()
    client_secret = Secret.load("ctrader-client-secret").get()
    access_token = Secret.load("ctrader-access-token").get()
    symbol_id = String.load("ctrader-symbol-id").value

    client = Client(
        EndPoints.PROTOBUF_LIVE_HOST
        if hostType.lower() == "live"
        else EndPoints.PROTOBUF_DEMO_HOST,
        EndPoints.PROTOBUF_PORT,
        TcpProtocol,
    )

    def connected(client):  # Callback for client connection
        logger.info("cTrader - Connected")
        request = ProtoOAApplicationAuthReq()
        request.clientId = client_id
        request.clientSecret = client_secret
        deferred = client.send(request)
        deferred.addErrback(on_error)

    def disconnected(_, reason):  # Callback for client disconnection
        logger.info("cTrader - Disconnected: ", reason)

    def on_message_received(_, message):  # Callback for receiving all messages
        if message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            logger.info("cTrader - API Application authorized")
            if account_id is not None:
                send_ProtoOAAccountAuthReq()
                return
        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            protoOAAccountAuthRes = Protobuf.extract(message)
            logger.info(
                f"cTrader - Account {protoOAAccountAuthRes.ctidTraderAccountId} has been authorized"
            )
            reactor.callLater(3, callable=send_ProtoOAReconcileReq)
        elif message.payloadType == ProtoOAReconcileRes().payloadType:
            positions = Protobuf.extract(message)
            parse_positions(positions)
            clean_exit()
        else:
            return

    def on_error(failure):
        logger.info("cTrader - Message Error: ", failure)
        clean_exit()

    def send_ProtoOAAccountAuthReq(clientMsgId=None):
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = account_id
        request.accessToken = access_token
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(on_error)

    def send_ProtoOAReconcileReq(clientMsgId=None):
        request = ProtoOAReconcileReq()
        request.ctidTraderAccountId = account_id
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(on_error)

    def clean_exit():
        logger.info("cTrader - Exiting")
        reactor.removeAll()
        reactor.iterate()
        reactor.stop()

    def parse_positions(positions):
        total_dot_size = 0
        total_swap = 0
        total_position_size = 0
        avg_entry_price = 0
        for position in positions.position:
            if position.tradeData.symbolId != int(symbol_id):
                continue
            moneyDigits = position.moneyDigits
            volume = position.tradeData.volume / 10**moneyDigits
            swap = position.swap / 10**moneyDigits
            price = position.price
            positionSize = volume * price

            total_dot_size += volume
            total_swap += swap
            total_position_size -= positionSize
        avg_entry_price = abs(total_position_size) / total_dot_size
        avg_entry_price = round(avg_entry_price, 5)
        total_swap = round(total_swap, 5)
        total_dot_size = round(total_dot_size, 5)
        total_position_size = round(total_position_size, 5)
        logger.info(f"cTrader - Total Dot Size: {total_dot_size}")
        logger.info(f"cTrader - Total Swap: {total_swap}")
        logger.info(f"cTrader - Total Position Size: {total_position_size}")
        logger.info(f"cTrader - Average Entry Price: {avg_entry_price}")
        update_prefect_block_data(
            avg_entry_price=avg_entry_price,
            total_swap=total_swap,
            total_dot_size=total_dot_size,
            total_position_size=total_position_size,
        )

    def update_prefect_block_data(
        avg_entry_price, total_dot_size, total_swap, total_position_size
    ):
        logger.info("cTrader - Updating Prefect Block Data")
        String(value=avg_entry_price).save(
            "ctrader-last-avg-entry-price", overwrite=True
        )
        String(value=total_dot_size).save("ctrader-last-total-dot-size", overwrite=True)
        String(value=total_swap).save("ctrader-last-total-swap", overwrite=True)
        String(value=total_position_size).save(
            "ctrader-last-total-position-size", overwrite=True
        )

    # Setting optional client callbacks
    client.setConnectedCallback(connected)
    client.setDisconnectedCallback(disconnected)
    client.setMessageReceivedCallback(on_message_received)

    # Starting the client service
    client.startService()
    reactor.run(installSignalHandlers=False)


if __name__ == "__main__":
    ctrader_get_positions_data()
