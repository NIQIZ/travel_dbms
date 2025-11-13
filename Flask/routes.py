from flask import request, jsonify, render_template
import os
from flask import Flask
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
from datetime import datetime
import sys


# CREATE
@app.route('/api/bookings', methods=['POST'])
def create_booking():
    data = request.json
    new_booking = { 'passenger_id': data['passenger_id'], 'flight_no': data['flight_no'], ... }
    result = mongo.db.bookings.insert_one(new_booking)
    return jsonify({ '_id': str(result.inserted_id) }), 201

# READ (list)
@app.route('/api/bookings', methods=['GET'])
def get_bookings():
    bookings = mongo.db.bookings.find()
    return jsonify([ { '_id': str(b['_id']), **b } for b in bookings ])

# UPDATE
@app.route('/api/bookings/<_id>', methods=['PUT'])
def update_booking(_id):
    data = request.json
    mongo.db.bookings.update_one({ '_id': ObjectId(_id) }, { '$set': data })
    return jsonify({ 'message': 'updated' })

# DELETE
@app.route('/api/bookings/<_id>', methods=['DELETE'])
def delete_booking(_id):
    mongo.db.bookings.delete_one({ '_id': ObjectId(_id) })
    return jsonify({ 'message': 'deleted' })
