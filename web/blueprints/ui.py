from flask import Blueprint, render_template

ui_bp = Blueprint("ui", __name__, template_folder="../templates", static_folder="../static")


@ui_bp.get("/")
def home():
    return render_template("ui.html", api_base="/api")
