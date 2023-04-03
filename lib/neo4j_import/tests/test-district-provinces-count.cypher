// Get all Districts
MATCH (d:District)
// Count the number of provinces a district is in
WITH d
CALL {
    WITH d
    MATCH (d)-[:IN_PROVINCE]->(p:Province)
    RETURN count(DISTINCT p) as nb_provinces
}
// Return the Districts which are in more than 1 Province (there should be none)
WITH d, nb_provinces
WHERE nb_provinces>1
RETURN d, nb_provinces
