from fota_server.application import create_app

app = create_app()


if __name__ == "__main__":
    import uvicorn
    from fota_server.config import PORT

    uvicorn.run(app, host="0.0.0.0", port=PORT)
