UNWIND $peaks AS row
// Update the Peaks nodes with its features
MERGE (peak:Peak {peakId: row.PEAKID})
SET peak.name = row.PKNAME,
    peak.alternateNames = CASE WHEN row.PKNAMES2 = "" THEN null ELSE row.PKNAMES2 END,
    peak.heightMeters = row.HEIGHTM,
    peak.heightFeet = row.HEIGHTF,
    peak.latitude = CASE WHEN row.LAT = "" THEN null ELSE toFloat(row.LAT) END,
    peak.longitude = CASE WHEN row.LON = "" THEN null ELSE toFloat(row.LON) END,
    peak.opened = row.OPEN,
    peak.unlisted = row.UNLISTED,
    peak.trekking = row.TREKKING,
    peak.hasBeenClimbed = CASE WHEN row.PCLIMBED = "" THEN null ELSE toBoolean(row.PCLIMBED) END,
    peak.trekkingYearAddition = CASE WHEN row.TREKYEAR = "" THEN null ELSE toInteger(row.TREKYEAR) END,
    peak.description = CASE WHEN row.DESCRIPTION = "" THEN null ELSE row.DESCRIPTION END,
    peak.memo = CASE WHEN row.PEAKMEMO = "" THEN null ELSE row.PEAKMEMO END,
    peak.referenceMemo = CASE WHEN row.REFERMEMO = "" THEN null ELSE row.REFERMEMO END,
    peak.photoMemo = CASE WHEN row.PHOTOMEMO = "" THEN null ELSE row.PHOTOMEMO END,
    peak.nepaleseFees = CASE WHEN row.NEPALESE_FEES = "" THEN null ELSE row.NEPALESE_FEES END,
    peak.foreignerFees = CASE WHEN row.FOREIGNER_FEES = "" THEN null ELSE row.FOREIGNER_FEES END
// Create Provinces, Districts and Ranges and their relationships
WITH peak, row
// Create the Province, the peak is in
FOREACH(ignoreMe IN CASE WHEN NOT row.PROVINCE = "" THEN [1] ELSE [] END |
    MERGE (province:Province {name: trim(row.PROVINCE)})
    // Create the District, the peak is in if there is one
    FOREACH(ignoreMe IN CASE WHEN NOT row.DISTRICT = "" THEN [1] ELSE [] END |
        // We check if there is a '/' in the district name, if so we create multiple nodes, one for each district
        FOREACH(ignoreMe IN CASE WHEN NOT row.DISTRICT CONTAINS "/" THEN [1] ELSE [] END |
            MERGE (d:District {name: trim(row.DISTRICT)})
            // Create the Peak->IN_DISTRICT->District->IN_PROVINCE->Province relationships
            MERGE (d)-[:IN_PROVINCE]->(province)
            MERGE (peak)-[:IN_DISTRICT]->(d))
        FOREACH(ignoreMe IN CASE WHEN row.DISTRICT CONTAINS "/" THEN [1] ELSE [] END |
            FOREACH(district in split(row.DISTRICT, "/") |
                // If the district is NC or NI, we don't create a node for it
                FOREACH(ignoreMe IN CASE WHEN (NOT district = "NC") AND (NOT district = "NI") THEN [1] ELSE [] END |
                    MERGE (d:District {name: trim(district)})
                    // Create a relationship between the Peak and Nepal Country
                    MERGE (c:Country {name: "Nepal"})
                    MERGE (peak)-[:IN_COUNTRY]->(c)
                    // Peaks can be at the border of two districts which is also the border of two provinces
                    // unfortunately in this case only one province is given, so in this case we do not create a
                    // relationship between the district and the province because it can create a wrong relationship
                    // and create districts which will be in two provinces
                    FOREACH(ignoreMe IN CASE WHEN NOT row.DISTRICT IN ["Darchula/Bajhang", "Dolpa/Mustang", "Dolpa/Myagdi",
                             "Dolpa/Rukum", "Dolakha/Solukhumbu", "Gorkha/Dhading", "Myagdi/Rukum", "Ramechhap/Solukhumbu",
                             "Humla/Bajhang"] THEN [1] ELSE [] END |
                        // Create the Peak->IN_DISTRICT->District->IN_PROVINCE->Province relationships
                        MERGE (d)-[:IN_PROVINCE]->(province)
                        MERGE (peak)-[:IN_DISTRICT]->(d))
                    // For the "Rukum East/Rukum" district, we create the relation between the first district and the province
                    // because there is no other peak with this district alone, so if we don't create this relation, the
                    // district will not be in any province
                    FOREACH(ignoreMe IN CASE WHEN row.DISTRICT = "Rukum East/Rukum" AND district = "Rukum East" THEN [1] ELSE [] END |
                        // Create the Peak->IN_DISTRICT->District->IN_PROVINCE->Province relationships
                        MERGE (d)-[:IN_PROVINCE]->(province)
                        MERGE (peak)-[:IN_DISTRICT]->(d)))
                // Create the Peak relationship with the China country
                FOREACH(ignoreMe IN CASE WHEN (district = "NC")THEN [1] ELSE [] END |
                    MERGE (c:Country {name: "China"})
                    MERGE (peak)-[:IN_COUNTRY]->(c))
                // Create the Peak relationship with the India country
                FOREACH(ignoreMe IN CASE WHEN (district = "NI")THEN [1] ELSE [] END |
                    MERGE (c:Country {name: "India"})
                    MERGE (peak)-[:IN_COUNTRY]->(c))
                ))))

// Create the Range, the peak is in
FOREACH(ignoreMe IN CASE WHEN NOT row.RANGE = "" THEN [1] ELSE [] END |
    MERGE (r:Range {name: row.RANGE})
    MERGE (peak)-[:IN_RANGE]->(r))