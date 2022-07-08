from flask import Flask, jsonify
from query_list import  *

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

@app.route("/")
def stock_project():
    return "This is Spark Assignment"

# Query - 1
@app.route("/get_max_moved_stock", methods=["GET"])
def max_diff():
    std = get_max_moved_stock()
    dict_to_return = {"Stock moved maximum percentage": std}
    return jsonify(dict_to_return)

# Query - 2
@app.route("/get_most_traded_stock", methods=["GET"])
def most_traded():
    std = get_most_traded_stock()
    dict_to_return = {"Most traded stock on each day": std}
    return jsonify(dict_to_return)

# Query - 3
@app.route("/get_max_gap", methods=["GET"])
def max_gap():
    std = get_max_gap()
    dict_to_return = {"Stock max gap up or down": std}
    return jsonify(dict_to_return)

# Query - 4
@app.route("/get_max_moved_stock", methods=["GET"])
def query4_api():
    std = get_max_moved_stock()
    dict_to_return = {"Maximum Moved stock": std}
    return jsonify(dict_to_return)

# Query - 5
@app.route("/get_standard_devaition", methods=["GET"])
def standard_deviation():
    std = get_standard_devaition()
    dict_to_return = {"Standard Deviation": std}
    return jsonify(dict_to_return)

# Query - 6
@app.route("/get_stock_mean_and_median", methods=["GET"])
def mean_and_median():
    std = get_stock_mean_and_median()
    dict_to_return = {"Mean and median": std}
    return jsonify(dict_to_return)

# Query - 7
@app.route("/get_avg_volume", methods=["GET"])
def avg_volume():
    std = get_avg_volume()
    dict_to_return = {"Average volume": std}
    return jsonify(dict_to_return)

# Query 8
@app.route("/get_highest_avg_volume", methods=["GET"])
def highest_avg_volume():
    std = get_highest_avg_volume()
    dict_to_return = {"Maximum average volume": std}
    return jsonify(dict_to_return)

# Query 9
@app.route("/get_highest_and_lowest_stock_price", methods=["GET"])
def highest_and_lowest_stock_price():
    std = get_highest_and_lowest_stock_price()
    dict_to_return = {"Highest and Lowest Stock Price": std}
    return jsonify(dict_to_return)

if __name__ == '__main__':
    app.run(debug=True)