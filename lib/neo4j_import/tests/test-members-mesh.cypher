// Get all expeditions
MATCH (e:Expedition)
WITH e
// Get the subgraph of the expedition and all its members
CALL apoc.path.subgraphNodes(e, {maxLevel:1}) YIELD node AS n
WHERE "Member" IN labels(n)
// For an expedition and a member, count the number of members in the expedition
WITH e, n
CALL {
    WITH e, n
    MATCH (m:Member)-[:LED|JOINED|WORKED_FOR]->(e)
    RETURN count(DISTINCT m) AS nb_members
}
// count the number of PARTNERED_WITH relationships between that member and the other members
// for the members part of the expedition
WITH e, n, nb_members
CALL {
    WITH e, n
    MATCH (m:Member)-[:PARTNERED_WITH]-(n)
    WHERE (m)-->(e)
    RETURN count(m) AS nb_partners
}
WITH e, n, nb_members, nb_partners
// Return the expedition and the member if the member has less or more partners than the
// total number of members in the expedition minus herself
// There should be no record returned
WHERE (nb_members-1 > 0 AND  nb_members-1 <> nb_partners) OR (nb_members=1 AND NOT nb_partners=0)
RETURN e, n, nb_members, nb_partners