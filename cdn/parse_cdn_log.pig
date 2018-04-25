data = load '/backup/cdn/$domain/$curdate/' using TextLoader();
resultlog = foreach data generate flatten(REGEX_EXTRACT_ALL($0,'^([0-9\\.]*) - - (\\[.*\\].*?) \\"GET (http:\\/\\/.*) HTTP\\/1\\.1\\" ([0-9]*) [0-9]*.*'));
result = filter resultlog by $3 == '404';
dump result;