var JSONStream = require("duplex-json-stream");
var EventEmitter = require("events").EventEmitter;

// 4 seconds
var maxDrift = 4 * 1000;

module.exports = DB;

/**
 * Creates a new DB connection for replication
 * @param {Stream}       stream The stream to use for replication
 * @param {Object}       data   Optional object to use as a store for the data
 * @param {EventEmitter} events Optional event emitter to use for events
 */
function DB(stream, data, events) {
	// Create the DB instance by basing it on an event emitter
	var db = Object.create(events || new EventEmitter());

	// Convert the stream to speak NDJson
	db.stream = JSONStream(stream);

	// Set the data store or create one
	db.data = data || {};

	db.peer = {
		// The delta in time calculated between the peer and the current node
		delta: 0,
		// The keys the peer is listening on
		listening: []
	};

	// Methods for working with the data
	db.put = put.bind(null, db);
	db.listen = listen.bind(null, db);

	// Listen on events coming from the peer
	listenPeer(db);

	return db;
}

/**
 * Sets a value in the local DB, notifies any listeners
 * @param  {DB}     db    The database instance to put into
 * @param  {String} key   The key that should be set
 * @param  {Any}    value The value to put under the key
 * @return {DB}           Returns the database for chaining
 */
function put(db, key, value) {
	// Generate a timestamp for this operation based on the current time
	var timestamp = Date.now();

	// Set the data in the data store locally
	// Save both the value and the timestamp
	db.data[key] = {
		value: value,
		timestamp: timestamp
	};

	// Notify any listeners of the update
	db.emit("key:" + key, value, timestamp);

	return db;
}

/**
 * Listens for changes in a given key
 * @param  {DB}                      db       The database instance to listen on changes from
 * @param  {String}                  key      The key to listen on
 * @param  {Function<String,Number>} listener A listener that gets notified with a new value and timestamp on updates
 * @return {DB}                               Returns the database for chaining
 */
function listen(db, key, listener) {
	// Adds a listener for changes in the key
	db.on("key:" + key, listener);

	// Notify the peer that you want to listen for changes in this key
	db.stream.write({
		method: "listen",
		key: key
	});

	// If the key already exists locally, send it off right away
	var existing = db.data[key];
	if (existing)
		db.emit("key:" + key, data.value, data.timestamp);

	return db;
}

/**
 * Listen from events coming from a peer
 * @param  {DB} db The DB to start listening for
 */
function listenPeer(db) {
	// Listen for objects sent down the stream
	db.stream.on("data", function(data) {
		// Catch all errors in processing and relay them to the db events
		try {
			// Handle the different methods being called
			if (data.method === "ping")
				handlePing(db, data);
			else if (data.method === "set")
				handleSet(db, data);
			else if (data.method === "listen")
				handleListen(db, data);
		} catch (e) {
			db.emit("error", e);
		}
	});

	// TODO: Only remove listeners added by peer
	db.stream.on("close", function() {
		db.emit("close");
		// Once the stream closes, just kill all listeners in the DB
		db.removeAllListeners();
		// #YOLO
	});
}

/**
 * Handle a ping message from the peer
 * @param  {DB}     db   The database this message is being sent to
 * @param  {Peer}   peer The data about the peer
 * @param  {Object} data The payload for the ping message
 */
function handlePing(db, data) {
	// Send a pong message to the peer with what the current time is
	// This will be used to calculate the delta in time which should be used for timestamps
	db.stream.write({
		method: "pong",
		pong: Date.now()
	});
}

/**
 * Handle a peer notifying of updated data
 * @param  {DB}     db   The database that the set is being sent to
 * @param  {Object} data The data from the set operation
 */
function handleSet(db, data) {
	var existing = db.data[data.key];
	// If the key hasn't been set before, take this one
	if (!existing) return handleNew(db, data);

	// Attempt to merge the new value with the local value
	handleConflict(db, existing, data);
}

/**
 * Handle a peer subscribing on some data
 * @param  {DB}     db   The database the listen is being sent to
 * @param  {Object} data The data from the listen
 */
function handleListen(db, data) {
	var key = data.key;

	// Already listening, don't duplicate
	if (db.peer.listening.indexOf(key) !== -1) return;

	// Track the listen
	db.peer.listening.push(key);

	// Track all changes to the key and send them to the peer
	db.listen(key, function(value, timestamp) {
		db.stream.write({
			method: "set",
			key: key,
			value: value,
			timestamp: timestamp
		});
	});
}

/**
 * Handle a new sets that should be persisted locally
 * @param  {DB} db   The database to persist to
 * @param  {Object} data The data for the set call
 */
function handleNew(db, data) {
	// Normalize the timestamp based on the delta in time between the peer
	var timestamp = normalizeTimestamp(db, data.timestamp);

	// Set the data locally
	db.data[key] = {
		value: data.value,
		timestamp: timestamp
	};

	// Notify any listeners of the change
	db.emit("key:" + key, value, timestamp);
}

/**
 * Handle cases where data exists locally but has been updated via the peer
 * @param  {DB} db       The database the conflict is happening on
 * @param  {Object} existing The local copy of the date
 * @param  {Object} updated  The new copy of the data from the peer
 */
function handleConflict(db, existing, updated) {
	// Normalize the timestamp of the new data based on the delta in time between the peer
	var updatedTime = normalizeTimestamp(db, updated.timestamp);
	// Use the local timestamp as is
	var existingTime = existing.timestamp;

	// Ignore update if a newer version exists locally
	if (updatedTime < existingTime) return;

	// Calculate how far in the future this might be
	var currentTime = Date.now();
	var drift = updatedTime - currentTime;

	// If the update is too far in the future from the local time, ignore it
	if (drift >= maxDrift) return;

	// Persist the newly updated data locally
	handleNew(db, updated);
}

function normalizeTimestamp(db, timestamp) {
	return timestamp - db.peer.delta;
}
