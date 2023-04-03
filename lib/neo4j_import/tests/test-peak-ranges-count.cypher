// Get all peaks
MATCH (p:Peak)
WITH p
CALL {
    WITH p
    MATCH (p)-[:IN_RANGE]->(r:Range)
    RETURN count(DISTINCT r) as nb_ranges
}
// Get the Peaks in more than 1 range (there should be none)
WITH p, nb_ranges
WHERE nb_ranges>1
RETURN p, nb_ranges