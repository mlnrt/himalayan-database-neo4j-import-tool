// Get all peaks
MATCH (p:Peak)
WHERE p.name IS NULL
// Return the peaks with no name attribute (there should be none)
RETURN p
