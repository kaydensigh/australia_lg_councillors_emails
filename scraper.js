// Environment variables:
// MORPH_API_KEY: API key used to fetch state databases.
// MORPH_STATE_DATABASES: comma-delimited list of scraper names.

"use strict";

var child_process = require('child_process');
var hashtable = require("hashtable");
var levenshtein = require("levenshtein");
var sqlite3 = require("sqlite3").verbose();

var EMAIL_REGEX = /[a-zA-Z0-9._%+-]{1,50}@[a-zA-Z0-9.-]{1,50}\.[a-z]{2,4}/g;
var USER_REGEX = /^[a-zA-Z0-9._%+-]+/;
var FIRST_REGEX = /^[a-z]+/;
var LAST_REGEX = /[a-z]+$/;
var MAX_SEARCH_RESULTS = 4;
var MAX_SEARCHES_PER_RUN = 100;

function initDatabase(callback) {
	var db = new sqlite3.Database("data.sqlite");
	db.serialize(function() {
		db.run("CREATE TABLE IF NOT EXISTS data (" +
			   "councillor TEXT, " +
			   "position TEXT, " +
			   "council_name TEXT, " +
			   "ward TEXT, " +
			   "council_website TEXT, " +
			   "email TEXT)");
		callback(db);
	});
}

// Update any rows that have results. If no email is found, the email field is
// set to "none" so that we don't try it again. The field is left blank if there
// was an error.
function updateAll(db, rows, results, callback) {
	console.log("Writing new email results to database.");
	for (var i = 0; i < rows.length; i++) {
		var row = rows[i];
		var result = results[keyFromRow(row)];
		if (!result)
			continue;

		if (result == "email") {
			db.run("UPDATE data SET email = ? WHERE rowid = ?",
				   row.email, i + 1);
		} else if (result == "no-email-found") {
			// None of the search results contained an email address.
			// Unfortunately, this is quite common.
			db.run("UPDATE data SET email = ? WHERE rowid = ?", "none", i + 1);
		}
	}
	callback();
}

// Insert |rows| as new rows into the database.
function insertNewRows(db, rows, callback) {
	console.log("Writing new rows to database.");
	var statement = db.prepare("INSERT INTO data VALUES (?, ?, ?, ?, ?, ?)");
	rows.forEach(function (row) {
		statement.run([row.councillor, row.position, row.council_name, row.ward,
					   row.council_website, row.email]);
		console.log("Added row: " + JSON.stringify(row));
	});
	statement.finalize();
	callback();
}

function readAll(db, callback) {
	db.all("SELECT * FROM data", function(err, rows) {
		callback(rows);
    });
}

function snippetToText(snippet) {
	// Remove <b> tags.
	return snippet.replace("<b>", "").replace("</b>", "");
}

function matchEmails(text) {
	return text.match(EMAIL_REGEX) || [];
}

function containsAny(haystack, list) {
	for (var i in list) {
		if (haystack.indexOf(list[i]) != -1)
			return true;
	}
	return false;
}

function trimUrl(url) {
	return url.replace(/^http:\/\//, "").replace(/\/$/, "");
}

function googleSearch(query, callback) {
	var url = "https://ajax.googleapis.com/ajax/services/search/web?v=1.0&q=" +
		encodeURIComponent(query);
	var child = child_process.exec(
		"curl -v -e http://www.oaf.org.au '" + url + "'",
		function (error, stdout, stderr) {
			if (error !== null) {
				console.log("Error making google query: " + error + "/n" +
							stderr + "/n");
				callback(null);
			}
			callback(JSON.parse(stdout));
		});
}

function fetchPage(url, callback) {
	// The default stdout buffer size is 200 * 1024, so we're effectively
	// ignoring pages larger than that.
	var child = child_process.exec(
		"curl -v -e http://www.oaf.org.au '" + url + "'",
		function (error, stdout, stderr) {
			if (error !== null) {
				console.log("Error requesting page: " + error + "/n" +
							stderr + "/n");
				callback("");
			}
			callback(stdout);
		});
}

function fetchDatabase(scraper, callback) {
	var url = "https://api.morph.io/" + scraper + "/data.json?" +
			"key=" + encodeURIComponent(process.env.MORPH_API_KEY) + "&" +
			"query=" + encodeURIComponent("SELECT * FROM data");
	var child = child_process.exec(
		"curl -v -e http://www.oaf.org.au '" + url + "'",
		{ maxBuffer: 500 * 1024 },
		function (error, stdout, stderr) {
			if (error !== null) {
				console.log("Error fetching database: " + error + "/n" +
							stderr + "/n");
				callback(null);
			}
			callback(JSON.parse(stdout));
		});
}

function scheduleGetEmailsInResult(results, index, emails, finalCallback) {
	setImmediate(function () {
		getEmailsInResult(results, index, emails, finalCallback);
	});
}

// Get the emails from a single result page. Calls itself for the next result
// page when done. Calls |finalCallback| when all result pages are done.
function getEmailsInResult(results, index, emails, finalCallback) {
	if (index == results.length || index >= MAX_SEARCH_RESULTS) {
		finalCallback(emails);
		return;
	}

	// Ignore anything that has a file format. It's usually PDF.
	// TODO: Improve this to handle anything that might have an email.
	if (results[index].fileFormat) {
		scheduleGetEmailsInResult(results, index + 1, emails, finalCallback);
		return;
	}
	fetchPage(results[index].url, function (html) {
		var emailsInResult = matchEmails(html);
		for (var e in emailsInResult)
			emails[emailsInResult[e]] = true;
		scheduleGetEmailsInResult(results, index + 1, emails, finalCallback);
	});
}

// For a set of google search results, pull all emails from the snippets and
// from the full text content of each result page.
function getEmailsFromSearch(results, callback) {
	var emails = {};
	for (var r in results) {
		var emailsInSnippet = matchEmails(
			snippetToText("<body>" + results[r].content + "</body>"));
		for (var e in emailsInSnippet)
			emails[emailsInSnippet[e]] = true;
	}
	scheduleGetEmailsInResult(results, 0, emails, callback);
}

// Return a tuple containing the result and the email with the closest
// Levenshtein distance to the name. Ignores case. This only considers emails
// that contain at least the first or last name. This is a good signal that the
// email is correct.
function getBestEmail(name, emails) {
	var nameLower = name.trim().toLowerCase();
	var firstOrLast = [];
	var first = nameLower.match(FIRST_REGEX);
	if (first)
		firstOrLast.push(first.toString());
	var last = nameLower.match(LAST_REGEX);
	if (last)
		firstOrLast.push(last.toString());

	var best = "";
	var bestDistance = 100000;
	for (var email in emails) {
		if (!containsAny(email, firstOrLast))
			continue;

		var user = email.toLowerCase().match(USER_REGEX).toString();
		var distance = new levenshtein(nameLower, user).distance;
		if (distance < bestDistance) {
			best = email;
			bestDistance = distance;
		}
	}

	if (best)
		return ["email", best];

	return ["no-email-found", ""];
}

// Get the email for a particular row. This does a web search constrained to
// the council's website and considers all emails in the top few search results.
function getEmail(row, results, callback) {
	googleSearch(row.councillor + " site:" + trimUrl(row.council_website),
		         function (result) {
		if (!result) {
			results[keyFromRow(row)] = "error-during-search";
			callback();
			return;
		}
		if (!result.responseData || !result.responseData.results) {
			results[keyFromRow(row)] = "no-search-results";
			callback();
			return;
		}
		getEmailsFromSearch(result.responseData.results, function (emails) {
			var result = getBestEmail(row.councillor, emails);
			row.email = result[1];
			results[keyFromRow(row)] = result[0];
			callback();
		});
	});
}

function printResult(row, results) {
	var result = results[keyFromRow(row)];
	if (result == "email") {
		console.log("Found email for " + row.councillor +
			        " (" + row.council_name + "): " + row.email);
	} else if (result == "no-email-found") {
		console.log("No email for " + row.councillor +
			        " (" + row.council_name + ")");
	} else if (result == "existing-email") {
		// Ignore.
	} else {
		// Log any other errors.
		console.log(result + " for " + row.councillor +
			        " (" + row.council_name + ")");
	}
}

function scheduleFindEmailForRow(rows, results, index, count, finalCallback) {
	setImmediate(function () {
		findEmailForRow(rows, results, index, count, finalCallback);
	});
}

function findEmailForRow(rows, results, index, count, finalCallback) {
	if (index >= rows.length) {
		finalCallback();
		return;
	}
	var row = rows[index];
	var next = function () {
		printResult(row, results);
		if (results[keyFromRow(row)] == "no-search-results" ||
			count == MAX_SEARCHES_PER_RUN ||
			index + 1 == rows.length) {
			finalCallback(count);
			return;
		}

		scheduleFindEmailForRow(rows, results, index + 1, count, finalCallback);
	};
	if (row.email) {
		results[keyFromRow(row)] = "existing-email";
		next();
		return;
	}
	if (!row.councillor) {
		results[keyFromRow(row)] = "no-councillor-name";
		next();
		return;
	}
	if (!row.council_website) {
		results[keyFromRow(row)] = "no-council-website";
		next();
		return;
	}
	count++;
	getEmail(row, results, next);
}

// Get the email for each row. Ignores rows that already have an email. Calls
// |callback| when finished, or |nothingToDo| if there was no work to do.
function findEmails(rows, results, callback, nothingToDo) {
	scheduleFindEmailForRow(rows, results, 0, 0, function (count) {
		if (count > 0) {
			console.log("Processed " + count + " rows.");
			callback();
		} else {
			nothingToDo();
		}
	});
}

function keyFromRow(row) {
	// The South Australia database has different column names.
	var name = row.councillor || row.name || ""; // Coerce falsy to "".
	var council = row.council_name || row.council || "";
	return (name + council).trim();
}

// Finds rows in |other| that are not in |rowMap| and appends them to |newRows|.
function mergeStateDatabase(newRows, rowMap, other) {
	var total = 0;
	other.forEach(function (otherRow) {
		var key = keyFromRow(otherRow);
		if (rowMap.has(key))
			return;
		var newRow = {
			councillor: otherRow.councillor || otherRow.name,
			position: otherRow.position,
			council_name: otherRow.council_name || otherRow.council,
			ward: otherRow.ward,
			council_website: otherRow.council_website || otherRow.council_url,
			email: otherRow.email,
		}
		newRows.push(newRow);
		rowMap.put(key, newRow);
		total++;
	});
	console.log("Added " + total + " new rows.");
}

function scheduleFetchStateDatabase(
		stateDatabases, index, newRows, rowMap, finalCallback) {
	setImmediate(function () {
		fetchStateDatabase(
			stateDatabases, index, newRows, rowMap, finalCallback);
	});
}

function fetchStateDatabase(
		stateDatabases, index, newRows, rowMap, finalCallback) {
	if (index == stateDatabases.length) {
		finalCallback();
		return;
	}
	var state = stateDatabases[index];
	fetchDatabase(state, function (data) {
		if (data) {
			console.log("Merging " + data.length + " rows from: " + state);
			mergeStateDatabase(newRows, rowMap, data);
		}
		scheduleFetchStateDatabase(
			stateDatabases, index + 1, newRows, rowMap, finalCallback);
	});
}

// Get all state databases listed in |MORPH_STATE_DATABASES| and add any new
// rows to our database.
function fetchAndMergeStateDatabases(rows, newRows, callback) {
	if (!process.env.MORPH_STATE_DATABASES || !process.env.MORPH_API_KEY) {
		console.log("Missing environment variable MORPH_STATE_DATABASES " +
					"and/or MORPH_API_KEY.");
		callback();
		return;
	}

	// Put our database into a hash map for quick lookup.
	var rowMap = new hashtable();
	rows.forEach(function (row) {
		rowMap.put(keyFromRow(row), row);
	});

	var stateDatabases =
		process.env.MORPH_STATE_DATABASES.split(",").filter(function (repo) {
			return repo.match(/^[A-Za-z0-9_\-\/]+$/);
		});
	console.log("Fetching state databases: ", stateDatabases);
	scheduleFetchStateDatabase(stateDatabases, 0, newRows, rowMap, callback);
}

function run() {
	initDatabase(function (db) {
		var closeDatabase = function () {
			console.log("Writing database to disk.");
			db.close(function () {
				var mem = (process.memoryUsage().rss / (1 << 20)).toFixed(2);
				console.log("Finished. Memory usage: " + mem + " MB");
			});
		};
		readAll(db, function (rows) {
			var results = new hashtable();
			// Do a normal search run. If there was nothing to do, pull new rows
			// from the state databases.
			findEmails(rows, results, function () {
				updateAll(db, rows, results, closeDatabase);
			}, function () {
				var newRows = [];
				fetchAndMergeStateDatabases(rows, newRows, function () {
					insertNewRows(db, newRows, closeDatabase);
				});
			});
		});
	});
}

run();
