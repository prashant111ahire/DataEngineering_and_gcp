--THIS FUNCTION CONVERTS STRING JSON DATA TO JSON KEEPING THE ORDER ( PARSE_JSON - also converts string to json but it reorders the output)
BEGIN
CREATE temp FUNCTION to_json(json_array STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  // Parse the JSON string into an array
  let jsonObjects = JSON.parse(json_array);
  
  // Initialize an array to store the json_items
  let result = [];
  
  // Iterate through the JSON array
  jsonObjects.forEach(item => {
    result.push(JSON.stringify(item)); // Add the entire JSON object as string
  });
  
  // Return the result as an array of JSON objects
  return result;
""";

select to_json('[{"sellingBrand":"NORDSTROM_RACK","logicalLocationId":"5629"}]');

END;
