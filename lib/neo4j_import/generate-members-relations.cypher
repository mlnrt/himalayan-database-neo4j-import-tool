MATCH (e:Expedition {expeditionId: $id, year: $year})
WITH e
// Get all the members of the expedition
MATCH (m:Member)-->(e)
// Generate a list of the members
WITH e, collect(DISTINCT m) AS members
// Get all combinations of pairs of members
WITH e, apoc.coll.combinations(members, 2) AS combos
// Unwind the list of pairs
UNWIND combos AS pair
// For each 2 members of the expedition
WITH pair[0] AS person1, pair[1] AS person2
// Find the existing relationships between the 2 members
OPTIONAL MATCH (person1)-[p1_p2:PARTNERED_WITH]->(person2)
OPTIONAL MATCH (person1)<-[p2_p1:PARTNERED_WITH]-(person2)
WITH person1, person2, p1_p2, p2_p1
FOREACH(ignoreMe IN CASE WHEN (p1_p2 IS NOT null) AND (p2_p1 IS null) THEN [1] ELSE [] END |
    SET p1_p2.expeditionCount = p1_p2.expeditionCount + 1)
FOREACH(ignoreMe IN CASE WHEN (p1_p2 IS null) AND (p2_p1 IS NOT null) THEN [1] ELSE [] END |
    SET p2_p1.expeditionCount = p2_p1.expeditionCount + 1)
FOREACH(ignoreMe IN CASE WHEN (p1_p2 IS null) AND (p2_p1 IS null) THEN [1] ELSE [] END |
    MERGE (person1)-[r:PARTNERED_WITH {expeditionCount: 1}]->(person2))