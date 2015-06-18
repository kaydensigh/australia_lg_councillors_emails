// Environment variables:
// MORPH_API_KEY: API key used to fetch state databases.
// MORPH_STATE_DATABASES: comma-delimited list of scraper names.

var cheerio = require("cheerio");
var hashtable = require("hashtable");
var levenshtein = require("levenshtein");
var request = require("request");
var sqlite3 = require("sqlite3").verbose();

var EMAIL_REGEX = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,4}/g;
var USER_REGEX = /^[a-zA-Z0-9._%+-]+/;
var FIRST_REGEX = /^[a-z]+/;
var LAST_REGEX = /[a-z]+$/;
var MAX_SEARCH_RESULTS = 10;
var MAX_SEARCHES_PER_RUN = 200;

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
function updateAll(db, all, callback) {
	console.log("Writing new email results to database.");
	for (var i = 0; i < all.length; i++) {
		var row = all[i];
		var result = row.result;
		if (!result)
			continue;

		if (result == "email") {
			db.run("UPDATE data SET email = ? WHERE rowid = ?",
				   row.email, i + 1);
			console.log("Added email for " + row.councillor +
				        " (" + row.council_name + "): " + row.email);
		} else if (result == "no-matching-email") {
			// There were no emails that contained the councillors name.
			// These don't ever seem to be correct, so mark them as "none".
			db.run("UPDATE data SET email = ? WHERE rowid = ?", "none", i + 1);
			console.log("No matching email for " + row.councillor +
				        " (" + row.council_name + "), closest was: " +
				        row.email);
		} else if (result == "no-email-found") {
			// None of the search results contained an email address.
			// Unfortunately, this is quite common.
			db.run("UPDATE data SET email = ? WHERE rowid = ?", "none", i + 1);
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

function htmlToText(html) {
	var $ = cheerio.load(html);
	return $("body").text();
}

function matchEmails(text) {
	return text.match(EMAIL_REGEX) || [];
}

function containsAny(haystack, list) {
	return list.filter(function (item) {
		return haystack.indexOf(item) != -1;
	}).length;
}

function trimUrl(url) {
	return url.replace(/^http:\/\//, "").replace(/\/$/, "");
}

function googleSearch(query, callback) {
	var options = {
		url: "https://ajax.googleapis.com/ajax/services/search/web",
		qs: {
			v: "1.0",
			q: query
		},
		headers: {
			"Referer": "http://www.oaf.org.au"
		}
	};
	request(options, function (error, response, body) {
		if (error) {
			console.log("Error making google query: " + error);
			callback(null);
			return;
		}

		callback(JSON.parse(body));
	});
}

function fetchPage(url, callback) {
	// Use request to read in pages.
	request(url, function (error, response, body) {
		if (error) {
			console.log("Error requesting page: " + error);
			callback("");
			return;
		}

		// Don't process the html with cheerio, it can cause a stack overflow.
		// Just matching match emails in the raw HTML should be mostly fine.
		callback(body);
	});
}

function fetchDatabase(scraper, callback) {
	var options = {
		url: "https://api.morph.io/" + scraper + "/data.json",
		qs: {
			key: process.env.MORPH_API_KEY,
			query: "SELECT * FROM data"
		}
	};
	request(options, function (error, response, body) {
		if (error) {
			console.log("Error fetching database: " + error);
			callback(null);
			return;
		}

		callback(JSON.parse(body));
	});
}

// Get the emails from a single result page. Calls itself for the next result
// page when done. Calls |finalCallback| when all result pages are done.
function getEmailsInResultPage(results, index, emails, finalCallback) {
	if (index == results.length || index >= MAX_SEARCH_RESULTS) {
		finalCallback(emails);
		return;
	}

	fetchPage(results[index].url, function (html) {
		emails.push.apply(emails, matchEmails(html));
		setImmediate(function () {
			getEmailsInResultPage(results, index + 1, emails, finalCallback);
		});
	});
}

// For a set of google search results, pull all emails from the snippets and
// from the full text content of each result page.
function getEmailsFromSearch(results, callback) {
	var emails = [];
	for (r in results) {
		emails.push.apply(emails, matchEmails(
			htmlToText("<body>" + results[r].content + "</body>")));
	}
	getEmailsInResultPage(results, 0, emails, callback);
}

// Return a tuple containing the result and the email with the closest
// Levenshtein distance to the name. Ignores case. This only considers emails
// that contain at least the first or last name.
function getBestEmail(name, emails) {
	var nameLower = name.trim().toLowerCase();
	var emailsByDistance = emails.map(function (email) {
		var user = email.toLowerCase().match(USER_REGEX).toString();
		var lev = new levenshtein(nameLower, user);
		return [lev.distance, email, user];
	});
	emailsByDistance.sort();

	var firstOrLast = [];
	var first = nameLower.match(FIRST_REGEX);
	if (first)
		firstOrLast.push(first.toString());
	var last = nameLower.match(LAST_REGEX);
	if (last)
		firstOrLast.push(last.toString());
	var matchingEmails = emailsByDistance.filter(function (levAndEmail) {
		return containsAny(levAndEmail[2], firstOrLast);
	});
	if (matchingEmails.length > 0) {
		return ["email", matchingEmails[0][1]];
	} else if (emailsByDistance.length > 0) {
		return ["no-matching-email", emailsByDistance[0][1]];
	}

	return ["no-email-found", ""];
}

// Get the email for a particular row. This does a web search constrained to
// the council's website and considers all emails in the top few search results.
function getEmail(row, callback) {
	googleSearch(row.councillor + " site:" + trimUrl(row.council_website),
		         function (result) {
		if (!result) {
			row.result = "error-during-search";
			callback();
			return;
		}
		if (!result.responseData || !result.responseData.results) {
			row.result = "no-search-results";
			callback();
			return;
		}
		getEmailsFromSearch(result.responseData.results, function (emails) {
			var result = getBestEmail(row.councillor, emails);
			row.email = result[1];
			row.result = result[0];
			callback();
		});
	});
}

// Get the email for each row, up to |MAX_SEARCHES_PER_RUN|. Ignores rows that
// already have an email. Calls |callback| when finished, or |nothingToDo| if
// there was no work to do.
function findEmails(rows, callback, nothingToDo) {
	var getEmailCount = 0;
	var first;
	var last;
	rows.forEach(function (row, index) {
		if (getEmailCount >= MAX_SEARCHES_PER_RUN)
			return;
		if (row.email) {
			row.result = "existing-email";
			return;
		}
		if (!row.councillor) {
			row.result = "no-councillor-name";
			return;
		}
		if (!row.council_website) {
			row.result = "no-council-website";
			return;
		}
		getEmailCount++;
		if (first === undefined)
			first = index;
		last = index;
		setImmediate(function () {
			getEmail(row, function () {
				getEmailCount--;
				if (getEmailCount == 0)
					callback();
			});
		});
	});
	if (getEmailCount > 0) {
		console.log("Getting emails for " + getEmailCount + " rows between " +
			        first + " and " + last + ".");
		return;
	}
	console.log("No rows to process.");
	nothingToDo();
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
		console.log(key);
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

// Get all state databases listed in |MORPH_STATE_DATABASES| and add any new
// rows to our database.
function fetchAndMergeStateDatabases(rows, newRows, callback) {
	// Put our database into a hash map for quick lookup.
	var rowMap = new hashtable();
	rows.forEach(function (row) {
		rowMap.put(keyFromRow(row), row);
	});

	var fetchDatabaseCount = 0;
	var stateDatabases =
		process.env.MORPH_STATE_DATABASES.split(",").filter(function (repo) {
			return repo.match(/^[A-Za-z0-9_\-\/]+$/);
		});
	console.log("Fetching state databases: ", stateDatabases);
	stateDatabases.forEach(function (state) {
		fetchDatabaseCount++;
		setImmediate(function () {
			fetchDatabase(state, function (data) {
				fetchDatabaseCount--;
				console.log("Merging from: " + state);
				mergeStateDatabase(newRows, rowMap, data);
				if (fetchDatabaseCount == 0)
					callback();
			});
		});
	});
}

function run() {
	initDatabase(function (db) {
		var closeDatabase = function () { db.close(); };
		readAll(db, function (rows) {
			// Do a normal search run. If there was nothing to do, pull new rows
			// from the state databases.
			findEmails(rows, function () {
				updateAll(db, rows, closeDatabase);
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
