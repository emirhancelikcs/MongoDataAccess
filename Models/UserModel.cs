using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDataAccess.Models;

public class UserModel
{
	//actually, our Id is like Guid
	[BsonId] //putting this two commands meaning we have an unique id, and it is our Id
	[BsonRepresentation(BsonType.ObjectId)]
	public string Id { get; set; }
	public string Name { get; set; }
	public string LastName { get; set; }
	public string FullName => $"{Name} {LastName}";//this is readonly
}
