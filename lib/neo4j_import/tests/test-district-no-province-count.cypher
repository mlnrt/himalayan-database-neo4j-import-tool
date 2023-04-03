// Get all districts
MATCH (d:District)
WITH d
// Count the number of provinces in which the district is located
CALL {
    WITH d
    MATCH (d)-[:IN_PROVINCE]->(p:Province)
    RETURN count(DISTINCT p) as nb_provinces
}
// Return the district if it is not located in any province (there should be none)
WITH d, nb_provinces
WHERE nb_provinces=0
RETURN d, nb_provinces