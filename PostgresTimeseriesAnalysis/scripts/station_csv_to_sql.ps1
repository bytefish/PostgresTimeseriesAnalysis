param (
    [string]$in_filename,
	[string]$out_filename
 )
 
function GetSqlCompatibleValue([string] $val)
{
	if([string]::IsNullOrWhiteSpace($val)) {
		return "NULL"
	}
	return $val.Replace("'", "''");
}


 
Import-Csv $in_filename -Delimiter "|" | Foreach-Object{

	$line = 
@"
`nINSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '{0}', '{1}', '{2}', '{3}', {4}, {5}, {6}, {7}, {8}
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='{0}');
"@ -f (GetSqlCompatibleValue $_.WBAN), (GetSqlCompatibleValue $_.Name), (GetSqlCompatibleValue $_.State), (GetSqlCompatibleValue $_.Location), (GetSqlCompatibleValue $_.Latitude), (GetSqlCompatibleValue $_.Longitude), (GetSqlCompatibleValue $_.GroundHeight), (GetSqlCompatibleValue $_.StationHeight), (GetSqlCompatibleValue $_.TimeZone)
	
	$line|Out-File $out_filename -Append 
	
}
	