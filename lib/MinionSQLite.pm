package MinionSQLite;
use Mojo::Base 'Mojolicious', -signatures;
use Mojo::File ('path');

# This method will run once at server start
sub startup ($self) {

  # Load configuration from config file
  my $config = $self->plugin(Config => {file => 'minion_sqlite.conf'});

  # Configure the application
  $self->secrets($config->{secrets});
  my $dbfile = path(__FILE__)->dirname->dirname->child('db') . '/' . $config->{db}->{dbname};
  $self->plugin(Minion => {SQLite => { dbfile => $dbfile }} ); 

  # $self->plugin('MinionSQLite::Task::Something');
  
  # Router
  my $r = $self->routes;

  # Normal route to controller
  $r->get('/')->to('Example#welcome');
}

1;
