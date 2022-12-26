import time

from ctrader_open_api import Client, EndPoints, Protobuf, TcpProtocol
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAAccountAuthRes,
    ProtoOAApplicationAuthReq,
    ProtoOAApplicationAuthRes,
    ProtoOADealListReq,
    ProtoOADealListRes,
    ProtoOAReconcileReq,
    ProtoOAReconcileRes,
    ProtoOARefreshTokenReq,
    ProtoOARefreshTokenRes,
    ProtoOATraderReq,
    ProtoOATraderRes,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOAPositionStatus,
    ProtoOATradeSide,
)
from prefect import get_run_logger, task
from prefect.blocks.system import Secret, String
from twisted.internet import reactor

startTimestamp = 1659359343000
endTimestamp = round(time.time() * 1000)

realized_pnl = 0
closed_swap = 0


@task(name="PPS Get Data Task")
def pps_get_all_data():
    logger = get_run_logger()

    hostType = "live"

    step = 604800000

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
        global startTimestamp
        global endTimestamp
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
            reactor.callLater(
                3,
                lambda: send_ProtoOADealListReq(
                    start=startTimestamp, end=startTimestamp + step
                ),
            )
        elif message.payloadType == ProtoOADealListRes().payloadType:
            deals = Protobuf.extract(message)
            agg_closed_deals(deals)
            if startTimestamp <= endTimestamp:
                reactor.callLater(
                    3,
                    lambda: send_ProtoOADealListReq(
                        start=startTimestamp, end=startTimestamp + step
                    ),
                )
                startTimestamp += step
            else:
                String(value=realized_pnl).save("pps-realized-pnl", overwrite=True)
                String(value=closed_swap).save("pps-closed-swap", overwrite=True)
                reactor.callLater(3, callable=send_ProtoOATraderReq)
        elif message.payloadType == ProtoOATraderRes().payloadType:
            trader_data = Protobuf.extract(message)
            get_account_balance(trader_data)
            clean_exit()
        else:
            content = Protobuf.extract(message)
            print(content)
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

    def send_ProtoOADealListReq(start, end, clientMsgId=None):
        request = ProtoOADealListReq()
        request.ctidTraderAccountId = account_id
        request.fromTimestamp = start
        request.toTimestamp = end
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
        String(value=pps_account_balance).save("pps-acct-balance", overwrite=True)

    def agg_closed_deals(deals):
        global realized_pnl
        global closed_swap

        for deal in deals.deal:
            if str(deal.closePositionDetail) == "" or deal.symbolId != int(symbol_id):
                continue
            moneyDigits = deal.closePositionDetail.moneyDigits
            closed_swap += deal.closePositionDetail.swap / 10**moneyDigits
            realized_pnl += deal.closePositionDetail.grossProfit / 10**moneyDigits

    def parse_positions(positions):
        open_dot_size = 0
        open_swap = 0
        open_positions_size = 0
        open_margin = 0
        open_dot_avg_price = 0

        for position in positions.position:
            if (
                position.tradeData.symbolId != int(symbol_id)
                or position.tradeData.tradeSide != ProtoOATradeSide.SELL
            ):
                continue
            if position.positionStatus == ProtoOAPositionStatus.POSITION_STATUS_OPEN:
                moneyDigits = position.moneyDigits
                volume = position.tradeData.volume / 10**moneyDigits
                swap = position.swap / 10**moneyDigits
                margin = position.usedMargin / 10**moneyDigits
                price = position.price
                positionSize = volume * price

                open_dot_size -= volume
                open_swap += swap
                open_positions_size += positionSize
                open_margin += margin
            else:
                continue

        if open_dot_size != 0:
            open_dot_avg_price = open_positions_size / abs(open_dot_size)
        else:
            open_dot_avg_price = 0

        String(value=open_margin).save("pps-open-margin", overwrite=True)
        String(value=open_dot_size).save("pps-open-dot-size", overwrite=True)
        String(value=open_dot_avg_price).save("pps-open-dot-avg-price", overwrite=True)
        String(value=open_swap).save("pps-open-swap", overwrite=True)

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
