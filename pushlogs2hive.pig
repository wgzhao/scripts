set default_parallel 8;
set job.name 'pushlogs to hive';
logs = load '${rtcdir}' using TextLoader();
-- logs.pushlogs
pushlogs = foreach logs generate flatten(REGEX_EXTRACT_ALL($0,'^([0-9-]* [0-9\\.:]*) \\[.*\\] <[0-9\\.]*>@.* (Replace|Close|Open) session ([0-9a-z]*)\\/.* (.*)$'));
pushlogs = filter pushlogs by $1 is not null;
pushlogs = foreach pushlogs generate $0,$2,$1,$3,'';
store pushlogs into '${pushlogstbl}' using PigStorage(',');
-- logs.pushacceptlogs
acceptlogs = foreach logs generate flatten(REGEX_EXTRACT_ALL($0,'^([0-9-]* [0-9\\.:]*) \\[.*\\] <[0-9\\.]*>@.* accept ([0-9\\.]*)->([0-9]*)$'));
acceptlogs = filter acceptlogs by $1 is not null;
acceptlogs = foreach acceptlogs generate $0 .. $2,'';
store acceptlogs into '${acceptlogtbl}' using PigStorage(',');
-- logs.pushfailauth
grp = foreach logs generate flatten(REGEX_EXTRACT_ALL($0,'^(?<statistime>[0-9-]* [0-9\\.:]*) \\[.*\\] <.*>@.* \\(.*\\) Failed authentication for (?<uuid>[0-9a-z]*)@push.haodou.com from IP (?<ip>[0-9\\.]*)$'));
grp = filter grp by $1 is not null;
pushfailauth = foreach grp generate $0,$1,$2,'';
store pushfailauth into '${pushauthfailtbl}' using PigStorage('\\u001');
