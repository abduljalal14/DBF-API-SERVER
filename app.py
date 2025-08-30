from flask import Flask
from controllers.order_controller import order_bp

app = Flask(__name__)

# Register blueprint
app.register_blueprint(order_bp)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
