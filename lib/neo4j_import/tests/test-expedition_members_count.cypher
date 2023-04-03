// Get all expeditions
MATCH (e:Expedition)
// Count the number of members of the expedition
WITH e
CALL {
    WITH e
    MATCH (m:Member)-[:LED|JOINED|WORKED_FOR]->(e)
    RETURN count(DISTINCT m) AS nb_members
}
// Return the expeditions with no members (there should be none)
WITH e, nb_members
WHERE nb_members = 0
RETURN e, nb_members
