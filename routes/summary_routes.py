from flask import Blueprint, jsonify, request
from analysis.summary import get_summary

# Utility for converting to native Python objects
from utils.serialization import convert_to_python_types

summary_blueprint = Blueprint('summary', __name__)

@summary_blueprint.route('/api/summary', methods=['GET'])
def summary_endpoint():
    """
    Example usage:
      GET /api/summary?symbol=QQQ
    Returns the analysis summary as JSON.
    """
    symbol = request.args.get('symbol', default='QQQ')
    try:
        df_summary = get_summary(symbol)
        df_summary = convert_to_python_types(df_summary)
        return jsonify(df_summary), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
