--FILMS TRIGGER
create function films_before_insert_trigger_func() returns trigger as $$
begin
perform * from countries where name=new.country;

if not found then
insert into countries values(new.country); 
end if;

perform 1 from films f join countries c on f.country=c.name
where f.id=new.id and f.name=new.name and f.year=new.year
and c.name=new.country;

if found then return null;
else return new;
end if;

end;
$$
language plpgsql;

create trigger films_before_insert_trigger before insert on films
for each row execute function films_before_insert_trigger_func();


--ACTORS TRIGGER
create function actors_before_insert_trigger_func() returns trigger as $$
begin
perform * from professions where name=new.name;

if not found then
insert into professions values(new.profession);
end if;

perform 1 from actors ac join professions pr on ac.profession=pr.name
where ac.name=new.name and ac.surname=new.surname 
and pr.name=new.profession;

if found then return null;
else return new;
end if;

end;
$$
language plpgsql;

create trigger actors_before_insert_trigger before insert on actors
for each row execute function actors_before_insert_trigger_func();

--ACTORSFILMS TRIGGERS
create function actorsfilms_before_update_delete() returns trigger as $$
begin
return null;
end;
$$
language plpgsql;

create trigger actorsfilms_before_update before update on actorsfilms
for each row execute function actorsfilms_before_update_delete();

create trigger actorsfilms_before_delete before delete on actorsfilms
for each row execute function actorsfilms_before_update_delete();

--PROFESSIONS TRIGGER
create function before_insert_professions_trigger_func() returns trigger as $$
begin
perform 1 from professions where name=new.name;
if found then
return null;
else return new;
end if;
end;
$$
language plpgsql;

create trigger before_insert_professions_trigger before insert on professions
for each row execute function before_insert_professions_trigger_func();


--COUNTRIES TRIGGER
create function before_insert_countries_trigger_func() returns trigger as $$
begin
perform 1 from countries where name=new.name;
if found then
return null;
else return new;
end if;
end;
$$
language plpgsql;

create trigger before_insert_countries_trigger before insert on countries
for each row execute function before_insert_countries_trigger_func();




