products = LOAD '/example/products/customer_records_map_reduce_input.json' USING JsonLoader('customerCategoryId:int,products:{(id:int,name:chararray,category:chararray,bought:boolean,price:double)}');

categories = LOAD '/example/dimension/customer_categories.db' AS (categoryId:int,age:chararray,gender:chararray);

joinedRecords = JOIN categories BY categoryId, products BY customerCategoryId;

--za svaku grupu korisnika prikazati top pet proizvoda koje grupa kupuje
flattenedProducts = FOREACH joinedRecords GENERATE
	                                           categories::categoryId AS categoryId,
	                                           categories::age AS age,
	                                           categories::gender AS gender,
	                                           FLATTEN(products);

generatedProducts = FOREACH flattenedProducts GENERATE
		                                           categoryId,
		                                           age,
		                                           gender,
		                                           products::id AS id,
		                                           products::name AS name,
		                                           products::category AS category,
		                                           products::bought AS bought,
		                                           products::price AS price;

boughtProducts = FILTER generatedProducts BY bought == true;

groupedProducts = GROUP boughtProducts BY (categoryId, age, gender, id, name);

countedProducts = FOREACH groupedProducts GENERATE 
                                               group.categoryId AS categoryId,
                                               group.age AS age,
                                               group.gender AS gender,
                                               group.id AS id,
                                               group.name AS name, 
                                               COUNT(boughtProducts) AS counter;

groupTopFiveProducts = GROUP countedProducts BY (categoryId, age, gender);

resultTopFiveProducts = FOREACH groupTopFiveProducts {
								                      sorted = ORDER countedProducts BY counter DESC;
								                      topProducts = LIMIT sorted 5;
								                      GENERATE
								                             FLATTEN(topProducts);
								                      };
         
STORE resultTopFiveProducts INTO '/example/results/topTenProducts' USING JsonStorage();

--prosecan broj pregledanih proizvoda (kupljeni ili ne) po poseti
averageSeenProducts = FOREACH joinedRecords GENERATE
	                                           categories::categoryId AS categoryId,
	                                           categories::age AS age,
	                                           categories::gender AS gender,
	                                           COUNT(products) AS counter;

grpAverageSeenProducts = GROUP averageSeenProducts BY (categoryId, age, gender);

averageCountedProducts = FOREACH grpAverageSeenProducts GENERATE
						                                    group.categoryId AS categoryId,
										    				group.age AS age,
						                                    group.gender AS gender,
						                                    AVG(averageSeenProducts.counter) AS averageSeen;

STORE averageCountedProducts INTO '/example/results/averageSeenProducts' USING JsonStorage();

--prosecan broj kupljenih proizvoda po poseti
averageBoughtProducts = FOREACH boughtProducts GENERATE
													categoryId,
													age,
													gender,
													TOBAG(name, id) AS products;

generatedCountedBoughtProducts = FOREACH averageBoughtProducts GENERATE
																	categoryId,
																	age,
																	gender,
																	COUNT(products) AS counter;

groupedAverageBoughtProducts = GROUP generatedCountedBoughtProducts BY (categoryId, age, gender);

resultAverageBoughtProducts = FOREACH groupedAverageBoughtProducts GENERATE
																		group.categoryId AS categoryId,
																		group.age AS age,
																		group.gender AS gender,
																		AVG(generatedCountedBoughtProducts.counter) AS averageBought;
STORE resultAverageBoughtProducts INTO '/example/results/averageBoughtProducts' USING JsonStorage();

--prosecna kolicina potrosenih para
groupedAveragePrice = GROUP boughtProducts BY (categoryId, age, gender);

averagePrice = FOREACH groupedAveragePrice GENERATE
                                            group.categoryId AS categoryId,
										    group.age AS age,
										    group.gender AS gender,
									 	    AVG(boughtProducts.price) AS averagePrice;

STORE averagePrice INTO '/example/results/averagePrice' USING JsonStorage();
                               
