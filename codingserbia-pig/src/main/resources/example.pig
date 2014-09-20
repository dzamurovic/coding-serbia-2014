products = LOAD '/example/products/*.json' USING JsonLoader('customerCategoryId:int,products:{(id:int,name:chararray,category:chararray,bought:boolean,price:double)}');

categories = LOAD '/example/dimension/customer_categories.db' AS (categoryId:int,from:int,to:int,gender:chararray);

joinedRecords = JOIN categories BY categoryId, products BY customerCategoryId;

--za svaku grupu korisnika prikazati top pet proizvoda koje grupa kupuje
flattenedProducts = FOREACH joinedRecords GENERATE
	                                           categories::categoryId AS categoryId,
	                                           categories::from AS ageFrom,
	                                           categories::to AS ageTo,
	                                           categories::gender AS gender,
	                                           FLATTEN(products);

generatedProducts = FOREACH flattenedProducts GENERATE
		                                           categoryId,
		                                           ageFrom,
		                                           ageTo,
		                                           gender,
		                                           products::id AS id,
		                                           products::name AS name,
		                                           products::category AS category,
		                                           products::bought AS bought,
		                                           products::price AS price;

boughtProducts = FILTER generatedProducts BY bought == true;

groupedProducts = GROUP boughtProducts BY (categoryId, ageFrom, ageTo, gender, id, name);

countedProducts = FOREACH groupedProducts GENERATE 
                                               group.categoryId AS categoryId,
                                               group.ageFrom AS ageFrom,
                                               group.ageTo AS ageTo,
                                               group.gender AS gender,
                                               group.id AS id,
                                               group.name AS name, 
                                               COUNT(boughtProducts) AS counter;

groupTopFiveProducts = GROUP countedProducts BY (categoryId, ageFrom, ageTo, gender);

resultTopFiveProducts = FOREACH groupTopFiveProducts {
								                      sorted = ORDER countedProducts BY counter DESC;
								                      topProducts = LIMIT sorted 5;
								                      GENERATE
								                             FLATTEN(topProducts);
								                      };
         
--DUMP resultTopFiveProducts;                    
--STORE resultTopFiveProducts INTO '/example/results/topTenProducts' USING JsonStorage();

--prosecan broj pregledanih proizvoda (kupljeni ili ne) po poseti
averageSeenProducts = FOREACH joinedRecords GENERATE
	                                           categories::categoryId AS categoryId,
	                                           categories::from AS ageFrom,
	                                           categories::to AS ageTo,
	                                           categories::gender AS gender,
	                                           COUNT(products) AS counter;

grpAverageSeenProducts = GROUP averageSeenProducts BY (categoryId, ageFrom, ageTo, gender);

averageCountedProducts = FOREACH grpAverageSeenProducts GENERATE
						                                    group.categoryId AS categoryId,
						                                    group.ageFrom AS ageFrom,
										    				group.ageTo AS ageTo,
						                                    group.gender AS gender,
						                                    AVG(averageSeenProducts.counter);

--DUMP averageCountedProducts;
--STORE averageCountedProducts INTO '/example/results/averageSeenProducts' USING JsonStorage();

--prosecan broj kupljenih proizvoda po poseti
averageBoughtProducts = FOREACH boughtProducts GENERATE
													categoryId,
													ageFrom,
													ageTo,
													gender,
													TOBAG(name, id) AS products;

generatedCountedBoughtProducts = FOREACH averageBoughtProducts GENERATE
																	categoryId,
																	ageFrom,
																	ageTo,
																	gender,
																	COUNT(products) AS counter;

groupedAverageBoughtProducts = GROUP generatedCountedBoughtProducts BY (categoryId, ageFrom, ageTo, gender);

resultAverageBoughtProducts = FOREACH groupedAverageBoughtProducts GENERATE
																		group.categoryId AS categoryId,
																		group.ageFrom AS ageFrom,
																		group.ageTo AS ageTo,
																		group.gender AS gender,
																		AVG(generatedCountedBoughtProducts.counter);
--DUMP resultAverageBoughtProducts;
--STORE resultAverageBoughtProducts INTO '/example/results/averageBoughtProducts' USING JsonStorage();

--prosecna kolicina potrosenih para
averagePrice = FOREACH groupedProducts GENERATE
                                            group.categoryId,
										    group.ageFrom,
										    group.ageTo,
										    group.gender,
									 	    AVG(boughtProducts.price);

--DUMP averagePrice;
--STORE averagePrice INTO '/example/results/averagePrice' USING JsonStorage();
                               
