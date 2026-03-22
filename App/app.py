"""
FE College Enrolment Forecasting Dashboard
Main Flask Application Entry Point
"""

import os
from flask import Flask, render_template

# Import configuration
from scripts.config import Config, config as config_map, validate_runtime_settings
from services.admin_jobs import BackgroundJobManager

# Import route blueprints
from routes.dashboard import dashboard as dashboard_bp
from routes.api import api as api_bp


def create_app(config_class=Config):
    """
    Application factory pattern for Flask app
    """
    app = Flask(__name__)
    app.config.from_object(config_class)
    validate_runtime_settings(app.config)
    app.extensions['job_manager'] = BackgroundJobManager(
        max_workers=app.config.get('ADMIN_JOB_MAX_WORKERS', 2)
    )
    
    # Ensure required directories exist
    os.makedirs(app.config['MODEL_DIR'], exist_ok=True)
    
    # Register blueprints
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(api_bp)
    
    # Error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        return render_template('error.html', 
                               error_code=404,
                               error_message="Page not found"), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        return render_template('error.html',
                               error_code=500,
                               error_message="Internal server error"), 500
    
    return app


def _get_config_class():
    """Resolve the active Flask config class from environment."""
    env_name = (os.environ.get('APP_ENV') or os.environ.get('FLASK_ENV') or 'default').lower()
    return config_map.get(env_name, Config)


# Create the application instance
app = create_app(_get_config_class())


if __name__ == '__main__':
    print("=" * 60)
    print("FE College Enrolment Forecasting Dashboard")
    print("=" * 60)
    print("\nStarting Flask development server...")
    print("Dashboard URL: http://localhost:5000")
    print("\nPress Ctrl+C to stop the server")
    print("-" * 60)
    
    app.run(host='0.0.0.0', port=5000, debug=app.config.get('DEBUG', False))
