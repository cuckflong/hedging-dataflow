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
    ProtoOARefreshTokenReq,
    ProtoOARefreshTokenRes,
    ProtoOATraderReq,
    ProtoOATraderRes,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAPositionStatus
from prefect import get_run_logger, task
from prefect.blocks.system import Secret, String
from twisted.internet import reactor


@task(name="PPS Get Data Task")
def pps_get_all_data():
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
            reactor.callLater(3, callable=send_ProtoOATraderReq)
        elif message.payloadType == ProtoOATraderRes().payloadType:
            trader_data = Protobuf.extract(message)
            get_account_balance(trader_data)
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

    def send_ProtoOATraderReq(clientMsgId=None):
        request = ProtoOATraderReq()
        request.ctidTraderAccountId = account_id
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(on_error)

    def clean_exit():
        logger.info("cTrader - Exiting")
        reactor.removeAll()
        reactor.iterate()
        reactor.stop()

    def get_account_balance(trader_data):
        moneyDigits = trader_data.trader.moneyDigits
        pps_account_balance = trader_data.trader.balance / 10**moneyDigits
        logger.info(f"PPS Account Balance: {pps_account_balance}")
        String(value=pps_account_balance).save("pps-last-acct-balance", overwrite=True)

    def parse_positions(positions):
        total_dot_size = 0
        total_swap = 0
        entry_position_size = 0
        avg_entry_price = 0
        for position in positions.position:
            if (
                position.tradeData.symbolId != int(symbol_id)
                or position.positionStatus != ProtoOAPositionStatus.POSITION_STATUS_OPEN
            ):
                continue
            moneyDigits = position.moneyDigits
            volume = position.tradeData.volume / 10**moneyDigits
            swap = position.swap / 10**moneyDigits
            price = position.price
            positionSize = volume * price

            total_dot_size += volume
            total_swap += swap
            entry_position_size -= positionSize

        avg_entry_price = abs(entry_position_size) / total_dot_size

        logger.info(f"PPS - Total Dot Size: {total_dot_size}")
        logger.info(f"PPS - Total Swap: {total_swap}")
        logger.info(f"PPS - Average Entry Price: {avg_entry_price}")

        String(value=avg_entry_price).save("pps-last-avg-entry-price", overwrite=True)
        String(value=total_dot_size).save("pps-last-total-dot-size", overwrite=True)
        String(value=total_swap).save("pps-last-total-swap", overwrite=True)

    # Setting optional client callbacks
    client.setConnectedCallback(connected)
    client.setDisconnectedCallback(disconnected)
    client.setMessageReceivedCallback(on_message_received)

    # Starting the client service
    client.startService()
    reactor.run(installSignalHandlers=False)


@task(name="PPS Token Refresh Task")
def pps_token_refresh():
    logger = get_run_logger()

    hostType = "live"

    account_id = int(Secret.load("ctrader-account-id").get())
    client_id = Secret.load("ctrader-client-id").get()
    client_secret = Secret.load("ctrader-client-secret").get()
    access_token = Secret.load("ctrader-access-token").get()
    refresh_token = Secret.load("ctrader-refresh-token").get()

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
            reactor.callLater(3, callable=send_ProtoOARefreshTokenReq)
        elif message.payloadType == ProtoOARefreshTokenRes().payloadType:
            refresh_msg = Protobuf.extract(message)
            logger.info("cTrader - Access Token refreshed")
            update_secrets(refresh_msg)
            clean_exit()
        else:
            return

    def on_error(failure):
        print("cTrader - Message Error: ", failure)
        clean_exit()

    def send_ProtoOAAccountAuthReq(clientMsgId=None):
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = account_id
        request.accessToken = access_token
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(on_error)

    def send_ProtoOARefreshTokenReq(clientMsgId=None):
        request = ProtoOARefreshTokenReq()
        request.refreshToken = refresh_token
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(on_error)

    def update_secrets(refresh_msg):
        logger.info("cTrader - Updating Prefect secrets")
        new_access_token = refresh_msg.accessToken
        new_refresh_token = refresh_msg.refreshToken
        Secret(value=new_access_token).save("ctrader-access-token", overwrite=True)
        Secret(value=new_refresh_token).save("ctrader-refresh-token", overwrite=True)

    def clean_exit():
        logger.info("cTrader - Exiting")
        reactor.removeAll()
        reactor.iterate()
        reactor.stop()

    # Setting optional client callbacks
    client.setConnectedCallback(connected)
    client.setDisconnectedCallback(disconnected)
    client.setMessageReceivedCallback(on_message_received)

    # Starting the client service
    client.startService()
    reactor.run(installSignalHandlers=False)
