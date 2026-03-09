from . import create_app
app = create_app()  # gunicorn will import "web.app:app"
