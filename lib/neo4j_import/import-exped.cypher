UNWIND $expeditions AS row
// Create the Expedition nodes with its features
MERGE (e:Expedition {
    expeditionId: row.EXPID,
    year: row.YEAR,
    season: row.SEASON_DESC,
    successClaimed: row.CLAIMED,
    successDisputed: row.DISPUTED,
    totalNbDays: row.TOTDAYS,
    terminationReason: row.TERMREASON_DESC,
    highpoint: row.HIGHPOINT,
    traverse: row.TRAVERSE,
    ski: row.SKI,
    parapente: row.PARAPENTE,
    camps: row.CAMPS,
    nbMembers: row.TOTMEMBERS,
    nbMembersSummit: row.SMTMEMBERS,
    nbMembersDeaths: row.MDEATHS,
    nbHiredPersonnel: row.TOTHIRED,
    nbHiredPersonnelSummit: row.SMTHIRED,
    nbHiredPersonnelDeaths: row.HDEATHS,
    noHiredPersonnelAboveBasecamp: row.NOHIRED,
    o2Used: row.O2USED,
    o2None: row.O2NONE,
    o2Climb: row.O2CLIMB,
    o2Descent: row.O2DESCENT,
    o2Sleep: row.O2SLEEP,
    o2Medical: row.O2MEDICAL,
    o2Taken: row.O2TAKEN,
    o2Unknown: row.O2UNKWN
})
ON CREATE
    SET e.name = row.EXPID + " " + row.YEAR,
        e.sponsor = CASE WHEN row.SPONSOR = "" THEN null ELSE row.SPONSOR END,
        e.approach = CASE WHEN row.APPROACH = "" THEN null ELSE row.APPROACH END,
        e.basecampDate = CASE WHEN row.BCDATE = "" THEN null ELSE date(row.BCDATE) END,
        e.summitDate = CASE WHEN row.SMTDATE = "" THEN null ELSE date(row.SMTDATE) END,
        e.summitTime = CASE WHEN row.SMTTIME = "" THEN null ELSE time(row.SMTTIME) END,
        e.terminationDate = CASE WHEN row.TERMDATE = "" THEN null ELSE date(row.TERMDATE) END,
        e.terminationNote = CASE WHEN row.TERMNOTE = "" THEN null ELSE row.TERMNOTE END,
        e.amountFixedRopes = CASE WHEN row.ROPE = "" THEN null ELSE toInteger(row.ROPE) END,
        e.otherSummits = CASE WHEN row.OTHERSMTS = "" THEN null ELSE row.OTHERSMTS END,
        e.campsite = CASE WHEN row.CAMPSITE = "" THEN null ELSE row.CAMPSITE END,
        e.routeMemo = CASE WHEN row.ROUTEMEMO = "" THEN null ELSE row.ROUTEMEMO END,
        e.accidents = CASE WHEN row.ACCIDENTS = "" THEN null ELSE row.ACCIDENTS END,
        e.achievements = CASE WHEN row.ACHIEVEMENTS = "" THEN null ELSE row.ACHIEVEMENTS END,
        e.standardRoute = CASE WHEN row.STDRTE = "" THEN null ELSE toBoolean(row.STDRTE) END

// Create the Peak Nodes. It should be merged later with data from the peak.csv data
MERGE (p:Peak {peakId: row.PEAKID})

WITH e, p, row
// Add a CommercialExpedition label to expedition nodes with the COMRTE property set to TRUE and the year is after 1987
FOREACH(ignoreMe IN CASE WHEN (NOT row.COMRTE = "") AND toBoolean(row.COMRTE) AND (e.year > 1987) THEN [1] ELSE [] END |
    SET e:CommercialExpedition)
// Add a NonCommercialExpedition label to expedition nodes with the COMRTE property set to FALSE
FOREACH(ignoreMe IN CASE WHEN (NOT row.COMRTE = "") AND (NOT toBoolean(row.COMRTE)) THEN [1] ELSE [] END |
    SET e:NonCommercialExpedition)
// Add a NonCommercialExpedition label to expedition post 1987 nodes with the COMRTE property not set if the PEAKID
// is not in the list of peaks known to have commercial routes
FOREACH(ignoreMe IN CASE WHEN (row.COMRTE = "") AND (e.year > 1987) AND (NOT row.PEAKID IN ["AMAD", "ANN4", "BARU", "CHOY", "EVER", "HIML", "MANA", "PUMO", "PUTH", "TILI"]) THEN [1] ELSE [] END |
    SET e:NonCommercialExpedition)
// Commercial expeditions started in 1988, so for all expeditions before that we set them as non-commercial by default
FOREACH(ignoreMe IN CASE WHEN (e.year < 1988) THEN [1] ELSE [] END |
    SET e:NonCommercialExpedition)
// Create the Agencies
FOREACH(ignoreMe IN CASE WHEN NOT row.AGENCY = "" THEN [1] ELSE [] END |
    MERGE (a:Agency {name: row.AGENCY})
    MERGE (e)-[:ORGANIZED_BY]->(a))
// Create the HOSTED_IN relationships
FOREACH(ignoreMe IN CASE WHEN NOT row.HOST_DESC = "" THEN [1] ELSE [] END |
    MERGE (c:Country {name: row.HOST_DESC})
    MERGE (e)-[:HOSTED_IN]->(c))
// If the ROUTE1 is not null and it was climbed,
// 1- We create a Route node. It has the PEAKID in the feature (not just a relation) to differentiate
// the SW RIDGE of mountain A from the SW RIDGE of mountain B.
// 2- As the climb was successful, the relation is 'CLIMBED' and the ascent number is attached to the relation.
// 3- The Route is linked to the Peak it is on.
// As a result, the nodes and relations state: "this is the Xth ascent of this route which is on that Peak"
FOREACH(ignoreMe IN CASE WHEN (NOT row.ROUTE1 = "") AND row.SUCCESS1 THEN [1] ELSE [] END |
    MERGE (r1:Route {name:row.ROUTE1 + " (" + row.PEAKID + ")"})
    MERGE (e)-[:CLIMBED{ascent:apoc.text.replace(coalesce(row.ASCENT1, 'Unknown'),'st.*|nd.*|rd.*|th.*', '')}]->(r1)
    MERGE (r1)-[:ON_PEAK]->(p))
// If the ascent of the Route was not successful, the relation is then ATTEMPTED
// In this case the relation has no "ascent" number since it has not been climbed.
FOREACH(ignoreMe IN CASE WHEN (NOT row.ROUTE1 = "") AND (NOT row.SUCCESS1) THEN [1] ELSE [] END |
    MERGE (r1:Route {name:row.ROUTE1 + " (" + row.PEAKID + ")"})
    MERGE (e)-[:ATTEMPTED]->(r1)
    MERGE (r1)-[:ON_PEAK]->(p))
// Same as above for the 2nd route of the expedition
FOREACH(ignoreMe IN CASE WHEN (NOT row.ROUTE2 = "") AND row.SUCCESS2 THEN [1] ELSE [] END |
    MERGE (r2:Route {name:row.ROUTE2 + " (" + row.PEAKID + ")"})
    MERGE (e)-[:CLIMBED{ascent:apoc.text.replace(coalesce(row.ASCENT2, 'Unknown'),'st.*|nd.*|rd.*|th.*', '')}]->(r2)
    MERGE (r2)-[:ON_PEAK]->(p))
FOREACH(ignoreMe IN CASE WHEN (NOT row.ROUTE2 = "") AND (NOT row.SUCCESS2) THEN [1] ELSE [] END |
    MERGE (r2:Route {name:row.ROUTE2 + " (" + row.PEAKID + ")"})
    MERGE (e)-[:ATTEMPTED]->(r2)
    MERGE (r2)-[:ON_PEAK]->(p))
// Same as above for the 3rd route of the expedition
FOREACH(ignoreMe IN CASE WHEN (NOT row.ROUTE3 = "") AND row.SUCCESS3 THEN [1] ELSE [] END |
    MERGE (r3:Route {name:row.ROUTE3 + " (" + row.PEAKID + ")"})
    MERGE (e)-[:CLIMBED{ascent:apoc.text.replace(coalesce(row.ASCENT3, 'Unknown'),'st.*|nd.*|rd.*|th.*', '')}]->(r3)
    MERGE (r3)-[:ON_PEAK]->(p))
FOREACH(ignoreMe IN CASE WHEN (NOT row.ROUTE3 = "") AND (NOT row.SUCCESS3) THEN [1] ELSE [] END |
    MERGE (r3:Route {name:row.ROUTE3 + " (" + row.PEAKID + ")"})
    MERGE (e)-[:ATTEMPTED]->(r3)
    MERGE (r3)-[:ON_PEAK]->(p))
// Same as above for the 4th route of the expedition
FOREACH(ignoreMe IN CASE WHEN (NOT row.ROUTE4 = "") AND row.SUCCESS4 THEN [1] ELSE [] END |
    MERGE (r4:Route {name:row.ROUTE4 + " (" + row.PEAKID + ")"})
    MERGE (e)-[:CLIMBED{ascent:apoc.text.replace(coalesce(row.ASCENT4, 'Unknown'),'st.*|nd.*|rd.*|th.*', '')}]->(r4)
    MERGE (r4)-[:ON_PEAK]->(p))
FOREACH(ignoreMe IN CASE WHEN (NOT row.ROUTE4 = "") AND (NOT row.SUCCESS4) THEN [1] ELSE [] END |
    MERGE (r4:Route {name:row.ROUTE4 + " (" + row.PEAKID + ")"})
    MERGE (e)-[:ATTEMPTED]->(r4)
    MERGE (r4)-[:ON_PEAK]->(p))
