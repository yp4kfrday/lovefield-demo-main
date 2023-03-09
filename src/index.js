const database = await buildSchema();
await printAllArtists();
await printAllAlbums();
await printAllRecords();
console.log(await getArtistsWithAlbum());
//console.log(await printAllRecordsWithoutReleaseDate());
//console.log(await printAllArtistsOnlyNameAndAlias());
//console.log(await printAllAlbumsWithoutIdAndArtist());
console.log(await printAllRecordsWithoutFeat());
console.log(await findRecordsWhereDurationMore(210));
console.log(await BornLaterThan1990(new Date("1989-01-27")));
console.log(await getAlbumWithRecords());
console.log(await getArtistsWithAlbumAndFeatNull());
await printSortAllArtists();
console.log(await getCountOfRecordsByAlbum());
console.log(await findNewestAlbum());
console.log(await avgDurationRecords());

await saveArtist({
  id: 1,
  alias: "The Beatles",
  fullName: "John Lennon",
  dateOfBirth: new Date("1980-01-29"),
});

await saveArtist({
  id: 2,
  alias: "Queen",
  fullName: "Bob Levandovski",
  dateOfBirth: new Date("1990-01-27"),
});

await saveArtist({
  id: 3,
  alias: "Led Zeppelin",
  fullName: "John Depp",
  dateOfBirth: new Date("1999-10-25"),
});

await saveAlbum({
  id: 1,
  nameAlbum: "Back in Black",
  releaseDate: new Date("1999-08-08"),
  artistNumber: 1,
});

await saveAlbum({
  id: 2,
  nameAlbum: "Highway to Hell",
  releaseDate: new Date("1999-10-08"),
  artistNumber: 1,
});

await saveAlbum({
  id: 3,
  nameAlbum: "Dark Horse",
  releaseDate: new Date("1999-01-11"),
  artistNumber: 2,
});

await saveAlbum({
  id: 4,
  nameAlbum: "Aerosmith",
  releaseDate: new Date("2000-02-13"),
  artistNumber: 2,
});

await saveAlbum({
  id: 5,
  nameAlbum: "Chinese Democracy",
  releaseDate: new Date("2002-03-12"),
  artistNumber: 3,
});

await saveAlbum({
  id: 6,
  nameAlbum: "Hysteria",
  releaseDate: new Date("2004-05-07"),
  artistNumber: 3,
});

await saveRecords({
  id: 1,
  name: "Whole Lotta Love",
  duration: 3 * 60 + 47,
  albumNumber: 1,
  feat: 2,
});

await saveRecords({
  id: 2,
  name: "Sympathy for the Devil",
  duration: 2 * 60 + 30,
  albumNumber: 2,
  feat: 4,
});

await saveRecords({
  id: 3,
  name: "Iron Man",
  duration: 4 * 60,
  albumNumber: 3,
  feat: 3,
});

await saveRecords({
  id: 4,
  name: "Barracuda",
  duration: 3 * 60 + 50,
  albumNumber: 4,
  feat: 2,
});

await saveRecords({
  id: 5,
  name: "Smoke on the Water",
  duration: 3 * 60 + 10,
  albumNumber: 5,
  feat: 2,
});

await saveRecords({
  id: 6,
  name: "Smells Like Teen Spirit",
  duration: 5 * 30 + 10,
  albumNumber: 6,
  feat: 1,
});

await saveRecords({
  id: 7,
  name: "We Will Rock You",
  duration: 2 * 60 + 40,
  albumNumber: 6,
  feat: 2,
});

await saveRecords({
  id: 8,
  name: "Paranoid",
  duration: 2 * 2 + 55,
  albumNumber: 6,
});

await saveRecords({
  id: 9,
  name: "Knocking on Heavens Door",
  duration: 60 * 3 + 20,
  albumNumber: 6,
});

await saveRecords({
  id: 10,
  name: "Personal Jesus",
  duration: 5 * 60 + 15,
  albumNumber: 6,
});

function printAllAlbums() {
  const albumTable = database.getSchema().table("album");

  return database.select().from(albumTable).exec().then(console.table);
}

function printAllArtists() {
  const artistTable = database.getSchema().table("artist");

  return database.select().from(artistTable).exec().then(console.table);
}

function printAllRecords() {
  const recordsTable = database.getSchema().table("records");

  return database.select().from(recordsTable).exec().then(console.table);
}

function saveArtist(artist) {
  const artistTable = database.getSchema().table("artist");
  const row = artistTable.createRow(artist);

  return database.insertOrReplace().into(artistTable).values([row]).exec();
}

function saveAlbum(album) {
  const albumTable = database.getSchema().table("album");
  const row = albumTable.createRow(album);

  return database.insertOrReplace().into(albumTable).values([row]).exec();
}

function saveRecords(records) {
  const recordsTable = database.getSchema().table("records");
  const row = recordsTable.createRow(records);

  return database.insertOrReplace().into(recordsTable).values([row]).exec();
}

function updateRecordsById(records) {
  const recordsTable = database.getSchema().table("records");

  return database
    .update(recordsTable)
    .set(recordsTable.name, records.name)
    .set(recordsTable.duration, records.duration)
    .set(recordsTable.albumNumber, records.albumNumber)
    .set(recordsTable.feat, records.feat)
    .where(recordsTable.id.eq(records.id))
    .exec();
}

function updateArtistAliasByIdBind(newAlias, artistId) {
  const artistTable = database.getSchema().table("artist");

  const query = database
    .update(artistTable)
    .set(artistTable.alias, lf.bind(0))
    .where(artistTable.id.eq(lf.bind(1)));

  return query.bind([newAlias, artistId]).exec();
}

function deleteRecordsById(recordsId) {
  const recordsTable = database.getSchema().table("records");

  return database
    .delete()
    .from(recordsTable)
    .where(recordsTable.id.eq(recordsId))
    .exec();
}

function deleteRecordsParameterById(recordsId) {
  const recordsTable = database.getSchema().table("records");

  const query = database
    .delete()
    .from(recordsTable)
    .where(recordsTable.id.eq(lf.bind(0)))
  return query.bind([recordsId])
    .exec();
}

/*явное внутреннее соединение*/

function getArtistsWithAlbum() {
  const artistTable = database.getSchema().table('artist');
  const albumTable = database.getSchema().table('album');

  return database
    .select()
    .from(artistTable)
    .innerJoin(albumTable, artistTable.id.eq(albumTable.artistNumber))
    .exec();
}

/* фильтрация */

function printAllRecordsWithoutFeat() {
  const recordsTable = database.getSchema().table("records");

  return database
    .select()
    .from(recordsTable)
    .where(recordsTable.feat.isNull())
    .exec();
}

function findRecordsWhereDurationMore(duration) {
  const recordsTable = database.getSchema().table('records');

  return database
    .select()
    .from(recordsTable)
    .where(recordsTable.duration.gt(duration))
    .exec();
}

function BornLaterThan1990(Date) {
  const artistTable = database.getSchema().table('artist');

  return database
    .select()
    .from(artistTable)
    .where(artistTable.dateOfBirth.gt(Date))
    .exec();
}

/*function printAllRecordsWithoutReleaseDate() {
  const recordsTable = database.getSchema().table("records");

  return (
    database
    .select(recordsTable.id, recordsTable.name, recordsTable.duration)
    .from(recordsTable)
    .exec()
    .then(console.table)
  );
}*/

/*function printAllArtistsOnlyNameAndAlias() {
  const artistTable = database.getSchema().table("artist");

  return (
    database
    .select(artistTable.alias, artistTable.fullName)
    .from(artistTable)
    .exec()
    .then(console.table)
  );
}*/

/*function printAllAlbumsWithoutIdAndArtist() {
  const albumTable = database.getSchema().table("album");

  return (
    database
    .select(albumTable.nameAlbum, albumTable.releaseDate)
    .from(albumTable)
    .exec()
    .then(console.table)
  );
}*/

/* левое соединение */

function getAlbumWithRecords() {
  const albumTable = database.getSchema().table('album');
  const recordsTable = database.getSchema().table('records');

  return database
    .select()
    .from(albumTable)
    .leftOuterJoin(recordsTable, albumTable.id.eq(recordsTable.albumNumber))
    .exec();
}

/* сложное соединение */

function getArtistsWithAlbumAndFeatNull() {
  const albumTable = database.getSchema().table('album');
  const recordsTable = database.getSchema().table('records');

  return database
    .select()
    .from(recordsTable)
    .innerJoin(albumTable, albumTable.id.eq(recordsTable.albumNumber))
    .where(
      lf.op.and(
        recordsTable.duration.gt(180),
        recordsTable.feat.isNull()
      )
    )
    .exec();
}

/*Сортировка*/

function printSortAllArtists() {
  const artistTable = database.getSchema().table("artist");

  return database
    .select()
    .from(artistTable)
    .orderBy(artistTable.fullName, lf.Order.ASC)
    .exec()
    .then(console.table);
}
/*Запросы с различной агрегацией*/

function getCountOfRecordsByAlbum() {
  const albumTable = database.getSchema().table("album")
  const recordsTable = database.getSchema().table("records")

  return database
    .select(albumTable.id.as("Album ID"), lf.fn.count(recordsTable.id).as("Count"))
    .from(albumTable)
    .leftOuterJoin(recordsTable, recordsTable.albumNumber.eq(albumTable.id))
    .groupBy(albumTable.id)
    .exec();
}

function findNewestAlbum() {
  const albumTable = database.getSchema().table("album");
  const artistTable = database.getSchema().table("artist");

  return database
    .select(artistTable.id.as("Artist ID"), lf.fn.max(albumTable.releaseDate).as("Newest"))
    .from(artistTable)
    .leftOuterJoin(albumTable, albumTable.artistNumber.eq(artistTable.id))
    .groupBy(artistTable.id)
    .exec();
}

function avgDurationRecords() {
  const albumTable = database.getSchema().table("album");
  const recordsTable = database.getSchema().table("records");

  return database
    .select(albumTable.id.as("Album ID"), lf.fn.avg(recordsTable.duration).as("Average duration"))
    .from(albumTable)
    .leftOuterJoin(recordsTable, recordsTable.albumNumber.eq(albumTable.id))
    .groupBy(albumTable.id)
    .exec();
}

function buildSchema() {
  const schemaBuilder = lf.schema.create("spotify_db", 25);

  schemaBuilder
    .createTable("artist")
    .addColumn("id", lf.Type.INTEGER)
    .addColumn("alias", lf.Type.STRING)
    .addColumn("fullName", lf.Type.STRING)
    .addColumn("dateOfBirth", lf.Type.DATE_TIME)
    .addPrimaryKey(["id"]);

  schemaBuilder
    .createTable("album")
    .addColumn("id", lf.Type.INTEGER)
    .addColumn("nameAlbum", lf.Type.STRING)
    .addColumn("releaseDate", lf.Type.DATE_TIME)
    .addColumn("artistNumber", lf.Type.INTEGER)
    .addPrimaryKey(["id"])
    .addForeignKey("fk_artist", {
      local: "artistNumber",
      ref: "artist.id",
    });

  schemaBuilder
    .createTable("records")
    .addColumn("id", lf.Type.INTEGER)
    .addColumn("name", lf.Type.STRING)
    .addColumn("duration", lf.Type.NUMBER)
    .addColumn("albumNumber", lf.Type.INTEGER)
    .addColumn("feat", lf.Type.INTEGER)
    .addNullable(["feat"])
    .addPrimaryKey(["id"])
    .addForeignKey("fk_album", {
      local: "albumNumber",
      ref: "album.id",
    })
    .addForeignKey("fk_feat", {
      local: "feat",
      ref: "artist.id",
    });

  return schemaBuilder.connect();
}