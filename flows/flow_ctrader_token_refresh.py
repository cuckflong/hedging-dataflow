from ctrader_open_api import Client, EndPoints, Protobuf, TcpProtocol
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAAccountAuthRes,
    ProtoOAApplicationAuthReq,
    ProtoOAApplicationAuthRes,
    ProtoOARefreshTokenReq,
    ProtoOARefreshTokenRes,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from prefect import flow, get_run_logger
from prefect.blocks.system import Secret, String
from twisted.internet import reactor


@flow(name="cTrader Token Refresh Flow")
def ctrader_token_refresh():
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


if __name__ == "__main__":
    ctrader_token_refresh()
