Hämtar filer och metadata från SND där forskningshuvudman är Sveriges lantbruksuniversitet.

I temp_mapp skapas filer som används för att hämta ner datat från SND.
I målmapp skapas Simpel Archive mapp med undermapp för varje samling som hämtas från SND och däri läggs eventuella filer samt:
contents fil, 
metadata i json format från SND,  
och dublin_core.xml.

När filerna är hämtade ska: 
Samling skapas i dspace (under den arkivbildare som anges i "archive_holder" kolumnen i snd_datalog.csv) för att få handle och collection_submit_id. 
contents filen uppdateras med collection_submit_id från dspace.

Import av samlingar körs enligt snabbguiden för Simple Archive i Dspace (https://archive-harvest.slu.se:10443/manuals/01-simparch.html)

För att köra scriptet:

$ ./get_snds.py  ~/temp_mapp ~/målmapp
