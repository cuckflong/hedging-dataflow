from prefect.blocks.system import Secret, String
from prefect.filesystems import RemoteFileSystem


def main():
    access_key = Secret.load("vultr-access-key").get()
    secret_key = Secret.load("vultr-access-secret").get()
    s3_url = String.load("vultr-s3-url").value

    hedge_pnl_block = RemoteFileSystem(
        basepath="s3://prefect/hedge-pnl",
        settings={
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {"endpoint_url": s3_url},
        },
    )
    hedge_pnl_block.save("storage-hedge-pnl")


if __name__ == "__main__":
    main()
