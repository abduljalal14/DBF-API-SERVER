from flask import Flask # type: ignore
from controllers.order_controller import order_bp
from controllers.product_controller import product_bp

app = Flask(__name__)

# Register blueprint
app.register_blueprint(order_bp)
app.register_blueprint(product_bp)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
