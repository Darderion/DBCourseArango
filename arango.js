const fs = require('fs');
const { Database } = require('arangojs');

let db = new Database({
	url: "http://127.0.0.1:8529",
});
db.useBasicAuth("root", "kNF6IpjdS00ESGT8");
db = db.database("NBA")

function sanitizeKey(key) {
	return key.replace(/[^a-zA-Z0-9-_:.]/g, '_');
}

class Player {
	constructor(
		name,
		team,
		age,
		height,
		weight,
		college,
		country,
		draft_year,
		draft_round,
		draft_number,
		gp,
		pts,
		reb,
		ast,
		net_rating,
		oreb_pct,
		dreb_pct,
		usg_pct,
		ts_pct,
		ast_pct,
		season
	) {
		this._key = sanitizeKey(name);
		this.name = sanitizeKey(name);
		this.team = sanitizeKey(team);
		this.age = parseFloat(age);
		this.height = parseFloat(height);
		this.weight = parseFloat(weight);
		this.college = sanitizeKey(college);
		this.country = sanitizeKey(country);
		this.draft_year = sanitizeKey(parseInt(draft_year, 10).toString());
		this.draft_round = parseInt(draft_round, 10);
		this.draft_number = parseInt(draft_number, 10);
		this.gp = parseInt(gp, 10);
		this.pts = parseFloat(pts);
		this.reb = parseFloat(reb);
		this.ast = parseFloat(ast);
		this.net_rating = parseFloat(net_rating);
		this.oreb_pct = parseFloat(oreb_pct);
		this.dreb_pct = parseFloat(dreb_pct);
		this.usg_pct = parseFloat(usg_pct);
		this.ts_pct = parseFloat(ts_pct);
		this.ast_pct = parseFloat(ast_pct);
		this.season = sanitizeKey(season);
	}
}

function populatePlayers(filePath) {
	const players = [];
	const fileContent = fs.readFileSync(filePath, 'utf-8');
	const lines = fileContent.split('\n');

	for (const line of lines) {
		if (line.trim() === '') continue;

		const fields = line.split(',');

		const player = new Player(
			fields[1],
			fields[2],
			fields[3],
			fields[4],
			fields[5],
			fields[6],
			fields[7],
			fields[8],
			fields[9],
			fields[10],
			fields[11],
			fields[12],
			fields[13],
			fields[14],
			fields[15],
			fields[16],
			fields[17],
			fields[18],
			fields[19],
			fields[20],
			fields[21]
		);
		players.push(player);
	}

	return players;
}

async function ensureCollection(name, type = "document") {
	const existing = await db.collection(name).get().catch(() => null);
	if (existing) {
		const isEdge = existing.type === 3;
		if ((type === "edge" && !isEdge) || (type === "document" && isEdge)) {
			await db.collection(name).drop();
		} else {
			return;
		}
	}

	if (type === "edge") {
		await db.collection(name).create({ type: 3 });
	} else {
		await db.collection(name).create();
	}
}

// Insert Data into ArangoDB
async function insertData(players) {
	const collections = ["Players", "College", "Team", "Draft", "Country", "Season"];
	const edges = ["PlayerToCollege", "PlayerToTeam", "PlayerToCountry", "PlayerToDraft", "PlayerToSeason"];

	// Ensure collections exist
	for (const collection of collections) {
		await ensureCollection(collection, "document");
	}

	for (const edge of edges) {
		await ensureCollection(edge, "edge");
	}

	console.time("InsertData");

	for (const player of players) {
		await db.collection("Players").save({ _key: player._key, name: player.name }, { overwrite: true });

		if (player.college) {
			await db.collection("College").save({ _key: player.college }, { overwrite: true });
			await db.collection("PlayerToCollege").save({
				_key: `${player._key}-${player.college}`,
				_from: `Players/${player._key}`,
				_to: `College/${player.college}`,
			}, { overwrite: true });
		}

		if (player.team) {
			await db.collection("Team").save({ _key: player.team }, { overwrite: true });
			await db.collection("PlayerToTeam").save({
				_key: `${player._key}-${player.team}-${player.season}`,
				_from: `Players/${player._key}`,
				_to: `Team/${player.team}`,
				season: player.season
			}, { overwrite: true });
		}

		if (player.country) {
			await db.collection("Country").save({ _key: player.country }, { overwrite: true });
			await db.collection("PlayerToCountry").save({
				_key: `${player._key}-${player.country}`,
				_from: `Players/${player._key}`,
				_to: `Country/${player.country}`,
			}, { overwrite: true });
		}

		if (player.draft_year) {
			await db.collection("Draft").save({ _key: player.draft_year }, { overwrite: true });
			await db.collection("PlayerToDraft").save({
				_key: `${player._key}-${player.draft_year}`,
				_from: `Players/${player._key}`,
				_to: `Draft/${player.draft_year}`,
			}, { overwrite: true });
		}

		if (player.season) {
			await db.collection("Season").save({ _key: player.season }, { overwrite: true });
			await db.collection("PlayerToSeason").save({
				_key: `${player._key}-${player.season}`,
				_from: `Players/${player._key}`,
				_to: `Season/${player.season}`,
				age: player.age,
				height: player.height,
				weight: player.weight,
				gp: player.gp,
				pts: player.pts,
				reb: player.reb,
				ast: player.ast,
				net_rating: player.net_rating
			}, { overwrite: true });
		}
	}

	console.timeEnd("InsertData");
}

async function clearDatabase() {
	const collections = await db.listCollections();
	for (const collection of collections) {
		try {
			console.log(`Dropping collection: ${collection.name}`);
			await db.collection(collection.name).drop();
		} catch (e) {
			console.error(`Failed to drop collection ${collection.name}:`, e.message);
		}
	}
}

(async function main() {
	const filePath = "all_seasons.csv";
	const playersList = populatePlayers(filePath);

	console.time("TotalExecution");

	console.log('Clearing database')
	await clearDatabase()

	console.log("Inserting data...");
	await insertData(playersList);

	console.timeEnd("TotalExecution");

	console.log("Data inserted successfully into ArangoDB!");
})();
