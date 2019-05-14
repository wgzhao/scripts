logs = load '$log' using TextLoader(); 
resultlog = foreach logs generate flatten(REGEX_EXTRACT_ALL($0,'^([^ ]*) - ([^ ]*) \\[(.*)\\] \\"([^ ]*) ([^ ]*) [^ ]*\\" (-|[0-9]*) (-|[0-9]*) \\"(.+?|-)\\" ([^ ]*|-) ([^ ]*|-) ([^ ]*|-) \\"(.+?|-)\\" \\"(.+?|-)\\" \\"(.+?|-)\\"$'));
store resultlog  into '$saved' using PigStorage('\\u001');