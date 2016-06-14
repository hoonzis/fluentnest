Write-Host "Starting elasticsearch script"

Invoke-WebRequest "https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-2.3.3.zip" -OutFile .\es.zip;

$destFolder = "$pwd\es\elasticsearch-2.3.3";

$shell = new-object -com shell.application;


$zip = $shell.NameSpace("$pwd\es.zip");

if (Test-Path $pwd\$destFolder )
{
	del $pwd\$destFolder -Force -Recurse
}

md ".\es";

foreach($item in $zip.items())
{
	$shell.Namespace("$pwd\es").copyhere($item);
}

cd $destFolder	

bin\plugin install delete-by-query

.\bin\service.bat install

.\bin\service.bat start

cd ..
cd ..