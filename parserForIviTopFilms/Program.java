package appjdbc;

import java.sql.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.concurrent.ExecutorCompletionService;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Program {
		
	public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException, LoaderException, ExecutionException 
	{	
		String address = "https://www.ivi.ru/collections/movies-highrated";
		
		var filmList = Loader.GetFilmList(address);	
		
		
		List<CompletableFuture<Film>> films = filmList.stream().map(Program::getFilm).toList();		
		
		CompletableFuture<Void> allFutures = CompletableFuture.allOf(films.toArray(new CompletableFuture[films.size()]));
		
	
		allFutures.join();		
		
			
	}
	
	
	static CompletableFuture<Film> getFilm(FilmInfo film) 
	{
		return CompletableFuture.supplyAsync(()->
		{
			Film f=null;
			try 
			{
				f=Loader.GetFilm(film);					
			}
			catch(URISyntaxException| InterruptedException| IOException| LoaderException ex)
			{
				System.out.println(ex.getMessage());
			}			
						
			if(f!=null) 
			{
				System.out.println(f.getName());
				saveFilmToDB(f);
			}
			return f;
		}
		);
		
	}
	
	static void saveFilmToDB(Film film) 
	{
		try 
		{
			var filmsTable="films";
			var actorsTable="actors";
			var actorsFilmsTable="actorsfilms";
			String url = "jdbc:postgresql://localhost:5432/postgres?user=myrole&password=1234";
			Connection con = DriverManager.getConnection(url);
			
			con.setAutoCommit(false);
			
			Statement st = con.createStatement();
			
			String queryInsertFilm=String.format("INSERT INTO %s VALUES('%s', '%s', '%d', '%d', '%d', '%s', '%s','"+film.getImage()+"')", 
					filmsTable, film.getId(), film.getName(), film.getYear(), film.getDuration(), film.getAgeCategory(),
					film.getCategory(), film.getCountry());
			
			System.out.println(queryInsertFilm);
			
			st.execute(queryInsertFilm);
			
			List<Actor> actors = film.getActors();
			
			String templateInsertActor = "INSERT INTO %s values('%s', '%s', '%s', '%s')";
			
			String templateInsertActorFilm = "INSERT INTO %s values('%s', '%s')";
			
			String templateCheckActor = "SELECT id from %s where name='%s' and surname='%s' and profession='%s'";
			
			for(Actor actor:actors)
			{				
				st.execute(String.format(templateInsertActor, actorsTable, actor.getId(), actor.getName(), actor.getSurname(), actor.getProfession()));
				
				ResultSet res = st.executeQuery(String.format(templateCheckActor, actorsTable, actor.getName(), actor.getSurname(), actor.getProfession()));
				
				String id;
				if(res.next())
				{
					id = res.getString(1);				
					st.execute(String.format(templateInsertActorFilm, actorsFilmsTable, id, film.getId()));	
				}
				else throw new Exception("Could not find actor");
			}
						
			con.commit();
			
			con.close();
		}
		catch(Exception ex)
		{
			System.out.println("DBException:"+ex.getMessage());
		}
		
	}
}



class SaverFilmToFile
{
	static Path dir = Path.of("c:", "test");
	static void saveFilmToFile(Film film) throws IOException
	{
		dir=dir.resolve(film.getName()+".txt");				
		
		var out = new PrintWriter(dir.toString());
		out.write(film.getName());
		out.println(film.getYear());
		out.println(film.getCountry());
		out.println(film.getDuration());
		out.println(film.getCategory());
		out.println(film.getAgeCategory());
		out.close();		
	}
	
	static void SaveImageOfFilm(Film film) throws IOException
	{
		dir=dir.resolve(film.getName()+".jpg");	
		try(var out = new FileOutputStream(dir.toString()))
		{
			out.write(film.getImage());
		}
	}
	
}


class Serializator
{
	final static Path address = Paths.get("c:","test","film.dat");
	static void saveFilm(Film film)
	{
		try(var out = new ObjectOutputStream(new FileOutputStream(address.toString())))
		{			
			out.writeObject(film);
		}
		catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}
	
	static Film readFilm()
	{
		Film film = null;
		try(var in = new ObjectInputStream(new FileInputStream(address.toString())))
		{			
			film = (Film) in.readObject();
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
		
		return film;
		
	}
	
}


class Loader
{
	
	static List<FilmInfo> GetFilmList(String address) throws InterruptedException, IOException, URISyntaxException, LoaderException
	{
		HttpClient client = HttpClient.newBuilder().followRedirects(Redirect.NORMAL).build();
		HttpRequest request = HttpRequest.newBuilder().uri(new URI(address)).GET().build();		
		HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
									
		if(response.statusCode()!=200) throw new LoaderException("Cannot get data from "+address);		
					
		Pattern pattern = Pattern.compile("<li.*?class=\"gallery__item\\sgallery__item_virtual\".*?>"
				+ "<a.*?href=\"/watch/(\\d+?)\".*?>"
				+ ".*?class=\"nbl-poster__image\".*?src=\"(.*?)\"/>.*?");
				
		
		Matcher matcher = pattern.matcher(response.body());	
		
		List<FilmInfo> films = new ArrayList<FilmInfo>();
		
		 matcher.results().forEach(x->films.add(new FilmInfo(Integer.parseInt(x.group(1)),x.group(2))));		 
		
		 
		 return films;		
	}
	
	static Film GetFilm(FilmInfo filmInfo) throws URISyntaxException, InterruptedException, IOException, LoaderException
	{
				
		URI uri = new URI("https://www.ivi.ru/watch/"+filmInfo.getId());
		HttpClient client = HttpClient.newBuilder().followRedirects(Redirect.NORMAL).build();
		HttpRequest request = HttpRequest.newBuilder().uri(uri).GET().build();
		HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
		
		
		Pattern pattern = Pattern.compile(
				"<div\\sclass=\"contentCard__info\">.*?"
				+ "<h1\\sclass=\"watchTitle__title.*?\">(.*?)</h1></div>" // 1 - name
				+ "<div.*?class=\"watchParams\\scontentCard__watchParams\">"
				+ "<ul.*?class=\"watchParams__paramsList\">"
				+ "<div.*?class=\"watchParams__item\">"
				+ "<a.*?>(.*?)</a></div>" // 2 - year
				+ "<div.*?class=\"watchParams__item\">"
				+ "<a.*?data-test=\"content-duration\".*?>"
				+ "(.*?)</a></div>"  // 3 - duration
				+ "<div.*?class=\"watchParams__item\">"
				+ "<a.*?>(.*?)</a></div>" // 4 - ageCategory
				+ "</ul>"
				+ "<ul.*?class=\"watchParams__paramsList\">"
				+ "<div.*?class=\"watchParams__item\\s+watchParams__item_hasDot\">"
				+ "<a.*?>(.*?)</a></div>" // 5 - country
				+ "<div.*?class=\"watchParams__item\\s+watchParams__item_hasDot\">"
				+ "<a.*?>(.*?)</a></div>" // 6 - category					
				);		
					
		Matcher matcher = pattern.matcher(response.body());
		
				
		if(!matcher.find()) return null;
		
				
		Pattern pattern2 = Pattern.compile("(\\d+)");
		Matcher matcher2 = pattern2.matcher(matcher.group(3));		
			
		if(!matcher2.find()) throw new LoaderException("Cannot parse the duration");		
		int duration = Integer.parseInt(matcher2.group(1))*60+Integer.parseInt(matcher.group(2)); // hours and mins to mins
		
		Matcher matcher3 = pattern2.matcher(matcher.group(4));
		
		if(!matcher3.find()) throw new LoaderException("Cannot parse age category");
		int ageCategory = Integer.parseInt(matcher3.group(1));
		
		
		Pattern pattern_actor = Pattern.compile(
				"fixedSlimPosterBlock__title\">(\\S+?)</div>.*?"
				+"fixedSlimPosterBlock__secondTitle\">(\\S+?)</div>.*?"
				+"fixedSlimPosterBlock__extra\">(\\S+?)</div>"
				);
		
		Matcher matcher_actor = pattern_actor.matcher(response.body());	
		
		byte [] image = null;
		
				
		try(InputStream in = new URI(filmInfo.getImageURI()).toURL().openStream())
		{			
			image = in.readAllBytes();			
			
		}
		
										
		Film film = new Film();
		
		film.setName(matcher.group(1));
		film.setYear(Integer.parseInt(matcher.group(2)));		
		film.setDuration(duration);
		film.setAgeCategory(ageCategory);
		film.setCountry(matcher.group(5));
		film.setCategory(matcher.group(6));	
		film.setImage(image);
		
		
		matcher_actor.results().forEach(o->
		{			
			Actor actor = new Actor(o.group(1), o.group(2), o.group(3), film);			
			film.addActor(actor);		
		});
		
				
		return film;				
	}
		
	
}

class LoaderException extends Exception
{
		LoaderException(String message)
		{
			super(message);
		}
}


class FilmInfo
{
	private int id;
	private String imageURI;
	
	FilmInfo(int id, String imageURI)
	{
		this.id=id;
		this.imageURI=imageURI;
	}
	
	int getId()
	{
		return this.id;
	}
	
	String getImageURI()
	{
		return this.imageURI;
	}
	
}


class Film implements Serializable
{
	private UUID id;
	private String name;
	private int year;
	private int duration;
	private int ageCategory;
	private String country;
	private String category;
	private List<Actor> actors;
	private byte [] image;
	
	
		
	Film()
	{
		id = UUID.randomUUID();
		this.actors = new ArrayList<Actor>();
	};
	
	Film(String name, int year, int duration, int ageCategory, String country, String category)
	{
		this();		
		this.name=name;
		this.year=year;
		this.duration=duration;
		this.ageCategory=ageCategory;
		this.country=country;
		this.category=category;			
	}
	
	void setName(String name)
	{
		this.name=name;
	}
	
	void setYear(int year)
	{
		this.year=year;
	}
	
	void setDuration(int duration)
	{
		this.duration=duration;
	}
	
	void setAgeCategory(int ageCategory)
	{
		this.ageCategory=ageCategory;
	} 
	
	void setCountry(String country)
	{
		this.country=country;
	}
	
	void setCategory(String category)
	{
		this.category=category;
	}
	
	void setImage(byte [] image)
	{
		this.image=image;
	}
	
	void addActor(Actor actor)
	{
		this.actors.add(actor);
	}
	
	UUID getId()
	{
		return this.id;
	}
	
	String getName()
	{
		return this.name;
	}
	
	int getYear()
	{
		return this.year;
	}
	
	int getDuration()
	{
		return this.duration;
	}
	
	int getAgeCategory()
	{
		return this.ageCategory;
	} 
	
	String getCountry()
	{
		return this.country;
	}
	
	String getCategory()
	{
		return this.category;
	}
	
	byte [] getImage()
	{
		return this.image;
	}
	
	List<Actor> getActors()
	{
		return actors;
	}

		
	
}

class Actor implements Serializable
{
	UUID id;
	private String name;
	private String surname;
	private String profession;	
	private List<Film> films;
	
	private Actor(String name, String surname, String profession)
	{
		id = UUID.randomUUID();
		this.name=name;
		this.surname=surname;
		this.profession=profession;
	}
	
	Actor(String name, String surname, String profession, List<Film> films)
	{
		this(name, surname, profession);
		this.films=films;
	}
	
	Actor(String name, String surname, String profession, Film film)
	{		
		this(name, surname, profession);
		this.films = new ArrayList<>();
		this.films.add(film);
	}
	
	UUID getId()
	{
		return id;
	}
	
	String getName()
	{
		return name;
	}
	
	String getSurname()
	{
		return surname;
	}
	
	String getProfession()
	{
		return profession;
	}
	
	
}


