create table countries
(	
	name text primary key
);

create table professions
(
	name text primary key
);

create table films
(
	id uuid primary key,
	name text not null check(length(name)>1),
	year int not null check(year>1900),
	duration int not null check(duration>0),
	ageCategory int not null check(ageCategory>=0),
	category text check(length(category)>0),
	country text references countries,	
	image bytea
);

create table actors
(
	id uuid primary key,
	name text check(length(name)>0),
	surname text check(length(surname)>0),
	profession text references professions	
);

create table actorsfilms
(
	actor uuid references actors on delete cascade on update cascade,
	film uuid references films on delete cascade on update cascade
);