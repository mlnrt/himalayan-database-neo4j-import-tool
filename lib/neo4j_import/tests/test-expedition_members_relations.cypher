// Get all expeditions
MATCH (e:Expedition)
WITH e
// Get the subgraph of the expedition and all its members
CALL apoc.path.subgraphNodes(e, {maxLevel:1}) YIELD node AS m
WHERE "Member" IN labels(m)
// For an expedition and a member, count the number of relationships between them
// there should be only one relationship of any type between a member and an expedition
// E.g. a member can have a LED and WORKED_FOR relationship with an expedition but not two LED relationships
// Count the number of LED relationships
WITH e, m
CALL {
    WITH e,m
    MATCH (m:Member)-[l:LED]->(e)
    RETURN count(l) AS nb_led
}
// Count the number of JOINED relationships
WITH e, m, nb_led
CALL {
    WITH e,m
    MATCH (m:Member)-[j:JOINED]->(e)
    RETURN count(j) AS nb_joined
}
// Count the number of WORKED_FOR relationships
WITH e, m, nb_led, nb_joined
CALL {
    WITH e,m
    MATCH (m:Member)-[w:WORKED_FOR]->(e)
    RETURN count(w) AS nb_worked_for
}
WITH e, m, nb_led, nb_joined, nb_worked_for
// Return the expedition and member if the member has more than one relationship of any type with the expedition
WHERE nb_led > 1 OR nb_joined > 1 OR nb_worked_for > 1
RETURN e, m, nb_led, nb_joined, nb_worked_for

