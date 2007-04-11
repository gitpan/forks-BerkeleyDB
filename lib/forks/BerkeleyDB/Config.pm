package
	forks::BerkeleyDB::Config;	#hide from PAUSE

$VERSION = 0.05;
use File::Spec;

use constant DEBUG => 0;
use constant ENV_ROOT => File::Spec->tmpdir().'/perlforks';
use constant ENV_PID => $$;	#would prefer $threads::SHARED, although current pid should be safe as long as it's main thread
use constant ENV_SUBPATH => int(ENV_PID / 100).'/'.ENV_PID.'/bdb';
use constant ENV_PATH => ENV_ROOT.'/'.ENV_SUBPATH;

1;
