var EventEmitter = require("events").EventEmitter;
var SubEmitter = require("subemitter");
var websocket = require("websocket-stream");
var express = require("express");

var DB = require("./DB");

var data = {};
var events = new EventEmitter();

var app = express();

var server = app.listen(process.env.PORT || 8282);

var wss = websocket.createServer({
	server: server
}, handleConnection);

function handleConnection(connection) {
	DB(connection, data, new SubEmitter(events));
}
