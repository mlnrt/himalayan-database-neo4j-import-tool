// We test that an expedition pre 1988, marked of having climbed a commercial route is still marked as a
// non-commercial expedition
MATCH (e:CommercialExpedition)
WHERE e.year < 1988
RETURN e
