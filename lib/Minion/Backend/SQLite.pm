package Minion::Backend::SQLite;
use Mojo::Base 'Minion::Backend', '-signatures';

use DBI;
use Mojo::JSON qw(decode_json encode_json);
use Mojo::IOLoop;
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

=pod
  return !!$self->pg->db->query(
    q{UPDATE minion_workers SET inbox = inbox || $1::JSONB WHERE (id = ANY ($2) OR $2 = '{}')},
    {json => [[$command, @$args]]}, $ids)->rows;
=cut
}

# Unclear how register_worker($worker_id) is supposed to work
sub register_worker ($self, $optionsHPtr={}) {
    my $db = $self->db;

    my $sql = sprintf(q{INSERT INTO minion_workers 
        (host, pid)
        VALUES (%s, %d)}, 
        hostname(), 
        $$);

    my $sth = $db->prepare($sql);
    if (!$sth->execute) {
        warn($db->errstr . "\n---\n$sql\n");
        return;
    }

    return $db->last_insert_id("", "", "", "");
}


sub dequeue ($self, $jobId=0, $waitInSeconds=5, $optionsHPtr={}) {

    # It there a pending job now?
    if ((my $job = $self->_getNextPendingJob($jobId, $optionsHPtr))) { 
        return $job;
    }
    return if Mojo::IOLoop->is_running;

    # Wait for another pending job to be available
    # This wait will not be interrupted by a new job hitting the queue.
    # We do not need this work queue to be that responsive
    my $db = $self->db;
    my $timer = Mojo::IOLoop->timer($waitInSeconds => sub { Mojo::IOLoop->stop });
    Mojo::IOLoop->start;
    $db->unlisten('*') and Mojo::IOLoop->remove($timer);

    # Last attempt to get a job until dequeue is called again in Minion
    return $self->_getNextPendingJob($jobId, $optionsHPtr);    
}

sub enqueue ($self, $task='default', $argsAPtr=[], $optionsHPtr={}) {
    my $db = $self->db;
    my $sql = sprintf(q[INSERT INTO minion_jobs 
    (args, attempts, notes, priority, task)
    VALUES
    (
        %s,
        %s,
        DATETIME('now', %s),
        %s
    ) 
    ],
        $db->db_quote(encode_json($argsAPtr)),
        ($optionsHPtr->{attempts} // 1),
        $db->db_quote('+' . ($optionsHPtr->{delay} // 0) . ' minute'),
        $db->db_quote(encode_json($optionsHPtr->{notes} // {})),
        ($optionsHPtr->{priority} // 100),
        $db->db_quote($task)
    );

    my $sth = $db->prepare($sql);
    if (!$sth->execute) {
        warn($db->errstr . "\n---\n$sql\n");
        return;
    }

    # Awkward, but canonical: 
    #   https://metacpan.org/pod/DBD::SQLite#$dbh-%3Esqlite_last_insert_rowid()
    return $db->last_insert_id("", "", "", "");

}

sub fail_job ($self, @args) {
}

sub finish_job ($self, @args) {
}

sub history ($self) {
=pod
  my $daily = $self->pg->db->query(
    "SELECT EXTRACT(EPOCH FROM ts) AS epoch, COALESCE(failed_jobs, 0) AS failed_jobs,
       COALESCE(finished_jobs, 0) AS finished_jobs
     FROM (
       SELECT EXTRACT (DAY FROM finished) AS day, EXTRACT(HOUR FROM finished) AS hour,
         COUNT(*) FILTER (WHERE state = 'failed') AS failed_jobs,
         COUNT(*) FILTER (WHERE state = 'finished') AS finished_jobs
       FROM minion_jobs
       WHERE finished > NOW() - INTERVAL '23 hours'
       GROUP BY day, hour
     ) AS j RIGHT OUTER JOIN (
       SELECT *
       FROM GENERATE_SERIES(NOW() - INTERVAL '23 hour', NOW(), '1 hour') AS ts
     ) AS s ON EXTRACT(HOUR FROM ts) = j.hour AND EXTRACT(DAY FROM ts) = j.day
     ORDER BY epoch ASC"
  )->hashes->to_array;

  return {daily => $daily};
=cut     
}

sub list_jobs ($self,  $offset=0, $limit=1000, $optionsHPtr={}) {
=pod

  my $jobs = $self->pg->db->query(
    q{SELECT id, args, attempts,
        ARRAY(SELECT id FROM minion_jobs WHERE parents @> ARRAY[j.id]) AS children,
        EXTRACT(epoch FROM created) AS created, EXTRACT(EPOCH FROM delayed) AS delayed,
        EXTRACT(EPOCH FROM expires) AS expires, EXTRACT(EPOCH FROM finished) AS finished, lax, notes, parents, priority,
        queue, result, EXTRACT(EPOCH FROM retried) AS retried, retries, EXTRACT(EPOCH FROM started) AS started, state,
        task, EXTRACT(EPOCH FROM now()) AS time, COUNT(*) OVER() AS total, worker
      FROM minion_jobs AS j
      WHERE (id < $1 OR $1 IS NULL) AND (id = ANY ($2) OR $2 IS NULL) AND (notes \? ANY ($3) OR $3 IS NULL)
        AND (queue = ANY ($4) OR $4 IS null) AND (state = ANY ($5) OR $5 IS NULL) AND (task = ANY ($6) OR $6 IS NULL)
        AND (state != 'inactive' OR expires IS null OR expires > NOW())
      ORDER BY id DESC
      LIMIT $7 OFFSET $8}, @$options{qw(before ids notes queues states tasks)}, $limit, $offset
  )->expand->hashes->to_array;

  return _total('jobs', $jobs);
=cut    
}

# This queue cannot be locked
sub list_locks ($self, $offset=0, $limit=1000, $optionsHPtr={}) {
    return;
}

sub list_workers ($self, $offset=0, $limit=1000, $optionsHPtr={}) {
=pod
  my $workers = $self->pg->db->query(
    "SELECT id, EXTRACT(EPOCH FROM notified) AS notified, ARRAY(
        SELECT id FROM minion_jobs WHERE state = 'active' AND worker = minion_workers.id
      ) AS jobs, host, pid, status, EXTRACT(EPOCH FROM started) AS started,
      COUNT(*) OVER() AS total
     FROM minion_workers
     WHERE (id < \$1 OR \$1 IS NULL)  AND (id = ANY (\$2) OR \$2 IS NULL)
     ORDER BY id DESC LIMIT \$3 OFFSET \$4", @$options{qw(before ids)}, $limit, $offset
  )->expand->hashes->to_array;
  return _total('workers', $workers);
=cut
}

# Sure, you have a lock, little Buddy.
sub lock ($self, $name, $duration, $optionsHPtr={}) {
    return 1;
}

# Locks are not supported
sub unlock ($self, $lockName) {
    return 1;
}

sub note ($self, $id, $merge) {
=pod
  return !!$self->pg->db->query('UPDATE minion_jobs SET notes = JSONB_STRIP_NULLS(notes || ?) WHERE id = ?',
    {json => $merge}, $id)->rows;
=cut
}

sub receive ($self, $jobId) {
=pod
  my $array = shift->pg->db->query(
    "UPDATE minion_workers AS new SET inbox = '[]'
     FROM (SELECT id, inbox FROM minion_workers WHERE id = ? FOR UPDATE) AS old
     WHERE new.id = old.id AND old.inbox != '[]'
     RETURNING old.inbox", shift
  )->expand->array;
  return $array ? $array->[0] : [];
=cut
}

sub remove_job ($self, $id) {

=pod
  return !!$self->pg->db->query(
    "DELETE FROM minion_jobs WHERE id = ? AND state IN ('inactive', 'failed', 'finished') RETURNING 1", $id)->rows;
=cut

}

sub repair ($self) {
}

sub reset ($self, $optionsHPtr={}) {
}

sub retry_job ($self, $id, $retries=0, $optionsHPtr={}) {
}

sub stats ($self) {
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

sub _takeJob ($self, $jobId, $optionsHPtr={}) {
    my $db = $self->db;
    my $sql = sprintf(qq[UPDATE minion_jobs 
    SET status='working',worker_id=%d,started_at=DATETIME('now') 
    WHERE id=%d AND worker_id IS NULL AND status='pending'],
        ('WORKER ID FIXME'),
        $jobId, 
    );

    my $sth = $db->prepare($sql);
    if (!$sth->execute) {
        warn($db->errstr . "\n---\n$sql\n");
        return;
    }

    return $jobId;
}


sub _getAllPendingJobs ($self, $jobId=0, $optionsHPtr={}) {
    my $db = $self->db;
    my $sql = qq[SELECT * FROM minion_jobs WHERE status='pending' ORDER BY priority ASC];
    if ($jobId) {
        $sql .= ' AND id=' . $jobId;
    }

    my $sth = $db->prepare($sql);
    if (!$sth->execute) {
        warn($db->errstr . "\n---\n$sql\n");
        return;
    }

    return $sth->fetchall_hashref;
}


sub _getNextPendingJob ($self, $jobId=0, $optionsHPtr={}) {
    my $jobs = $self->_getAllPendingJobs($jobId, $optionsHPtr);
    for my $job (@$jobs) {
        if ($self->_takeJob($job->{id})) {
            return $job->{id};
        }
    }
    return;
}

1;
