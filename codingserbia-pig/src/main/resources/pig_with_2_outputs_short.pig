products = LOAD '/example/products/customer_records_map_reduce_input.json' USING JsonLoader('sessionId:int,customerCategoryId:int,customerCategoryDescripton:chararray,products:{(id:int,name:chararray,category:chararray,bought:boolean,price:double)}');

categories = LOAD '/example/dimension/customer_categories.db' AS (categoryId:int,age:chararray,gender:chararray);

joinedRecords = JOIN categories BY categoryId, products BY customerCategoryId;

--za svaku grupu korisnika prikazati top pet proizvoda koje grupa kupuje
flattenedProducts = FOREACH joinedRecords GENERATE
											   sessionId AS sessionId,
											   categories::categoryId AS categoryId,
											   categories::age AS age,
											   categories::gender AS gender,
											   FLATTEN(products.(id, name, category, bought, price)) AS (id, name, category, bought, price);

boughtProducts = FILTER flattenedProducts BY bought == true;

groupedProducts = GROUP boughtProducts BY (categoryId, age, gender, id, name);

countedProducts = FOREACH groupedProducts GENERATE 
											   FLATTEN(group),
											   COUNT(boughtProducts) AS counter;

groupTopFiveProducts = GROUP countedProducts BY (categoryId, age, gender);

resultTopFiveProducts = FOREACH groupTopFiveProducts {
													  sorted = ORDER countedProducts BY counter DESC;
													  topProducts = LIMIT sorted 5;
													  GENERATE
															 FLATTEN(topProducts);
													  };
		 
STORE resultTopFiveProducts INTO '/example/results/topTenProducts' USING JsonStorage();

--prosecan broj pregledanih proizvoda (kupljeni ili ne) 
averageSeenProducts = FOREACH joinedRecords GENERATE
											   categories::categoryId AS categoryId,
											   categories::age AS age,
											   categories::gender AS gender,
											   COUNT(products) AS counter;

grpAverageSeenProducts = GROUP averageSeenProducts BY (categoryId, age, gender);

averageCountedProducts = FOREACH grpAverageSeenProducts GENERATE
															FLATTEN(group),
															AVG(averageSeenProducts.counter) AS averageSeen;

--prosecan broj kupljenih proizvoda po poseti
groupedBySession = GROUP boughtProducts BY (sessionId, categoryId, age, gender);

averageBoughtProducts = FOREACH groupedBySession GENERATE
													FLATTEN(group),
													COUNT(boughtProducts.name) AS counter;
													
groupedAverageBoughtProducts = GROUP averageBoughtProducts BY (categoryId, age, gender);

resultAverageBoughtProducts = FOREACH groupedAverageBoughtProducts GENERATE
																		FLATTEN(group),
																		AVG(averageBoughtProducts.counter) AS averageBought;

--prosecna kolicina potrosenih para
groupedAveragePrice = GROUP boughtProducts BY (categoryId, age, gender);

averagePrice = FOREACH groupedAveragePrice GENERATE
											FLATTEN(group),
											AVG(boughtProducts.price) AS averagePaid;

joinedFinal = JOIN averageCountedProducts BY (categoryId, age, gender), resultAverageBoughtProducts BY (categoryId, age, gender), averagePrice BY (categoryId, age, gender);

finalResult = FOREACH joinedFinal GENERATE
										averageCountedProducts::categoryId AS categoryId,
										averageCountedProducts::age AS age,
										averageCountedProducts::gender AS gender,
										averageCountedProducts::averageSeen AS averageSeen,
										resultAverageBoughtProducts::averageBought AS averageBought,
										averagePrice::averagePaid AS averagePaid;

 STORE finalResult INTO '/example/results/productsStatistic' USING JsonStorage();