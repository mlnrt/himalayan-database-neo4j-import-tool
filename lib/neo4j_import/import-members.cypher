UNWIND $members AS row
// Create the People nodes with its features
MERGE (m:Member {
    personId: row.PERSID,
    firstName: row.FNAME,
    lastName: row.LNAME,
    gender: row.SEX,
    yearOfBirth: toInteger(row.YOB)
})
// Add a name display property to the node
ON CREATE
    SET m.name = row.LNAME + " " + row.FNAME,
    m.residence = CASE WHEN row.RESIDENCE = "" THEN null ELSE row.RESIDENCE END,
    m.occupation = CASE WHEN row.OCCUPATION = "" THEN null ELSE row.OCCUPATION END
// We reset the residence and occupation properties as we find new entries for the same person
// So a person will have it's latest occupation and residence in the database
ON MATCH
    SET m.residence = CASE WHEN row.RESIDENCE = "" THEN m.residence ELSE row.RESIDENCE END,
        m.occupation = CASE WHEN row.OCCUPATION = "" THEN m.occupation ELSE row.OCCUPATION END
// Create the Country Nodes.
// We check if there is a '/' in the country name, if so we create multiple nodes, one for each country
FOREACH(ignoreMe IN CASE WHEN NOT row.CITIZEN CONTAINS "/" THEN [1] ELSE [] END |
    MERGE (c:Country {name: row.CITIZEN})
    MERGE (m)-[:CITIZEN_OF]->(c))
FOREACH(ignoreMe IN CASE WHEN row.CITIZEN CONTAINS "/" THEN [1] ELSE [] END |
    FOREACH(country in split(row.CITIZEN, "/") |
        MERGE (c:Country {name: country})
        MERGE (m)-[:CITIZEN_OF]->(c)))
WITH m, row
// Add a Sherpa label to member nodes with the SHERPA column set to true
FOREACH(ignoreMe IN CASE WHEN row.SHERPA THEN [1] ELSE [] END |
    SET m:Sherpa)
// Add a Tibetan label to member nodes with the TIBETAN column set to true
FOREACH(ignoreMe IN CASE WHEN toBoolean(row.TIBETAN) THEN [1] ELSE [] END |
    SET m:Tibetan)
// Add a NonSherpaNonTibetan label to member nodes with the SHERPA and TIBETAN column set to false
FOREACH(ignoreMe IN CASE WHEN (NOT toBoolean(row.TIBETAN)) AND (NOT toBoolean(row.SHERPA)) THEN [1] ELSE [] END |
    SET m:NonSherpaNonTibetan)