package
	forks::BerkeleyDB::Config;	#hide from PAUSE

$VERSION = 0.03;
use File::Spec;

use constant DEBUG => 1;
use constant ENV_ROOT => File::Spec->tmpdir().'/perlforks';
use constant ENV_PATH => ENV_ROOT.'/env.'.$$;	#would prefer $threads::SHARED, although current pid should be safe as long as it's main thread

1;
