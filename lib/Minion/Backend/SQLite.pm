package Minion::Backend::SQLite;
use Mojo::Base 'Minion::Backend', '-signatures';

use DBI;
use Sys::Hostname ('hostname');

has dbfile => sub { '' };

has db_exists => sub { 0 };

has db => sub ($self) {
    my $dbfile = $self->dbfile;
    if (!$dbfile) {
        die("Please define the name of the file in minion_sqlite.conf");
    }

    my $db = DBI->connect("dbi:SQLite:dbname=$dbfile","","");
    $db->{AutoCommit} = 1;

    if (!$self->db_exists) {
        my $sth = $db->table_info(undef, 'main', 'minion_jobs', 'TABLE');
        if (!$sth->execute) {
            die($db->errstr);
        }
        $self->db_exists(!!$sth->rows);
    }

    return $db;
};


sub new ($class, $inArgs={}) {
    my $new = $class->SUPER::new(%$inArgs);

    my $db = $new->db;
    if (!$new->db_exists) {
        $new->db_exists($new->_create_tables);        
    }

    return $new;
}

sub broadcast ($self, $command, $argsAPtr=[], $idsAPtr=[])  {
}

sub dequeue ($self, $id, $wait, $optionsHPtr={}) {
}

sub enqueue ($self, $task, $argsAPtr=[], $optionsHPtr={}) {
}

sub fail_job ($self, @args) {
}

sub finish_job ($self, @args) {
}

sub history ($self) {
}

sub list_jobs ($self,  $offset=0, $limit=1000, $optionsHPtr={}) {
}

sub list_locks ($self, $offset=0, $limit=1000, $optionsHPtr={}) {
}

sub list_workers ($self, $offset=0, $limit=1000, $optionsHPtr={}) {
}

sub lock ($self, $name, $duration, $optionsHPtr={}) {
}

sub note ($self, $id, $merge) {
}

sub receive ($self, $jobId) {
}

sub register_worker ($self, $id, $optionsHPtr={}) {
}

sub remove_job ($self, $id) {
}

sub repair ($self) {
}

sub reset ($self, $optionsHPtr={}) {
}

sub retry_job ($self, $id, $retries=0, $optionsHPtr={}) {
}

sub stats ($self) {
}

sub unlock ($self, $lockName) {
}

sub unregister_worker ($self, $id) {
}

# DB functions
sub _create_tables ($self) {

    # Table definitions based on:
    # https://fastapi.metacpan.org/source/SRI/Minion-10.31/lib/Minion/Backend/resources/migrations/pg.sql
    my @tableDefs = (
                  qq[CREATE TABLE IF NOT EXISTS minion_jobs (
id INT PRIMARY KEY,
status VARCHAR(15) CHECK( status IN ('pending', 'working', 'completed', 'failed')) NOT NULL DEFAULT('pending'),
args JSON NOT NULL,
task TEXT NOT NULL,
worker_id INT,
attempts INT NOT NULL DEFAULT 1,
started_at DATETIME,
finished_at DATETIME,
notes JSON NOT NULL DEFAULT '{}',
priority INT NOT NULL DEFAULT 100,
result JSON,
updated_at DATETIME NOT NULL,
created_at DATETIME NOT NULL
)
],
                  qq[CREATE TABLE IF NOT EXISTS minion_workers (
id INT PRIMARY KEY,
host VARCHAR(255) NOT NULL,
pid INT NOT NULL,
status VARCHAR(16) CHECK(status IN ('active', 'inactive')) NOT NULL DEFAULT('active'),
inbox JSON,
notified DATETIME,
updated_at DATETIME NOT NULL,
created_at DATETIME NOT NULL
    )
],
                 );
    my $db = $self->db;
    for my $tableDef (@tableDefs) {
        my $sth = $db->prepare($tableDef);
        if (!$sth->execute()) {
            warn("Could not create:\n---\n$tableDef\n---\n");
            exit(1);
        }
    }
    return 1;
}


1;
