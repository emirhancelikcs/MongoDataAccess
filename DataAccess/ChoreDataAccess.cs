using MongoDataAccess.Models;
using MongoDB.Driver;

namespace MongoDataAccess.DataAccess;

public class ChoreDataAccess
{
	private const string ConnectionString = "your address";
	private const string DatabaseName = "choredb";
	private const string ChoreCollection = "chore_chart";
	private const string UserCollection = "users";
	private const string ChoreHistoryCollection = "chore_history";

	private IMongoCollection<T> ConnectToMongo<T>(in string collection)
	{
		MongoClient client = new MongoClient(ConnectionString);//connect to server
		IMongoDatabase db = client.GetDatabase(DatabaseName);//connect to database
		return db.GetCollection<T>(collection);//crud
	}

	public async Task<List<UserModel>> GetAllUsers()
	{
		IMongoCollection<UserModel> usersCollection = ConnectToMongo<UserModel>(UserCollection);
		IAsyncCursor<UserModel> results = await usersCollection.FindAsync(_ => true);
		return results.ToList();
	}

	public async Task<List<ChoreModel>> GetAllChores()
	{
		IMongoCollection<ChoreModel> choresCollection = ConnectToMongo<ChoreModel>(ChoreCollection);
		IAsyncCursor<ChoreModel> results = await choresCollection.FindAsync(_ => true);
		return results.ToList();
	}

	public async Task<List<ChoreModel>> GetAllChoresForAUser(UserModel user)
	{
		IMongoCollection<ChoreModel> choresCollection = ConnectToMongo<ChoreModel>(ChoreCollection);
		IAsyncCursor<ChoreModel> results = await choresCollection.FindAsync(c => c.AssignedTo.Id == user.Id);
		return results.ToList();
	}

	public Task CreateUser(UserModel user)
	{
		IMongoCollection<UserModel> usersCollection = ConnectToMongo<UserModel>(UserCollection);
		return usersCollection.InsertOneAsync(user);
	}

	public Task CreateChore(ChoreModel chore)
	{
		IMongoCollection<ChoreModel> choresCollection = ConnectToMongo<ChoreModel>(ChoreCollection);
		return choresCollection.InsertOneAsync(chore);
	}

	public Task UpdateChore(ChoreModel chore)
	{
		IMongoCollection<ChoreModel> choresCollection = ConnectToMongo<ChoreModel>(ChoreCollection);
		FilterDefinition<ChoreModel> filter = Builders<ChoreModel>.Filter.Eq("Id", chore.Id);
		return choresCollection.ReplaceOneAsync(filter, chore, new ReplaceOptions { IsUpsert = true });
		//IsUpsert means look for Replace/Update, and if there is no recored, insert new one!
	}

	public Task DeleteChore(ChoreModel chore)
	{
		IMongoCollection<ChoreModel> choresCollection = ConnectToMongo<ChoreModel>(ChoreCollection);
		return choresCollection.DeleteOneAsync(c => c.Id == chore.Id);
	}

	public async Task CompleteChore(ChoreModel chore)
	{
		//IMongoCollection<ChoreModel> choresCollection = ConnectToMongo<ChoreModel>(ChoreCollection);
		//FilterDefinition<ChoreModel> filter = Builders<ChoreModel>.Filter.Eq("Id", chore.Id);
		//await choresCollection.ReplaceOneAsync(filter, chore);

		//IMongoCollection<ChoreHistoryModel> choreHistoryCollection = ConnectToMongo<ChoreHistoryModel>(ChoreHistoryCollection);
		//await choreHistoryCollection.InsertOneAsync(new ChoreHistoryModel(chore));

		MongoClient client = new MongoClient(ConnectionString);
		using IClientSessionHandle session = await client.StartSessionAsync();
		//using is use when this model end (in this model is CompleteChore)

		session.StartTransaction();
		//Transaction means if you have 2 or more job to do and doing this jobs one by one, so if first job completed with no error complete second, and if second job completed with no error, do next, and there is no error at the end, complete success, if there is an error during jobs, abort and revert any job, like didnt started

		try
		{
			IMongoDatabase db = client.GetDatabase(DatabaseName);
			IMongoCollection<ChoreModel> choresCollection = db.GetCollection<ChoreModel>(ChoreCollection);
			FilterDefinition<ChoreModel> filter = Builders<ChoreModel>.Filter.Eq("Id", chore.Id);
			await choresCollection.ReplaceOneAsync(filter, chore);

			IMongoCollection<ChoreHistoryModel> choreHistoryCollection = db.GetCollection<ChoreHistoryModel>(ChoreHistoryCollection);
			await choreHistoryCollection.InsertOneAsync(new ChoreHistoryModel(chore));

			await session.CommitTransactionAsync();
			//complete transaction
		}
		catch (Exception ex)
		{
			await session.AbortTransactionAsync();//cancel transaction
			Console.WriteLine(ex.Message);
		}
	}
}
