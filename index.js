const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    try {
        await migrationUp();

        
        // async function getPopulationData() {
        // const response = await axios.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population');
        // return response.data.data;
        // // return response
        // }
        
        // getPopulationData().then(data => console.log(data));
        // console.log('data:', data.data);
  
        const data = {"data":[{"ID Nation":"01000US","Nation":"United States","ID Year":2020,"Year":"2020","Population":326569308,"Slug Nation":"united-states"},{"ID Nation":"01000US","Nation":"United States","ID Year":2019,"Year":"2019","Population":324697795,"Slug Nation":"united-states"},{"ID Nation":"01000US","Nation":"United States","ID Year":2018,"Year":"2018","Population":322903030,"Slug Nation":"united-states"},{"ID Nation":"01000US","Nation":"United States","ID Year":2017,"Year":"2017","Population":321004407,"Slug Nation":"united-states"},{"ID Nation":"01000US","Nation":"United States","ID Year":2016,"Year":"2016","Population":318558162,"Slug Nation":"united-states"},{"ID Nation":"01000US","Nation":"United States","ID Year":2015,"Year":"2015","Population":316515021,"Slug Nation":"united-states"},{"ID Nation":"01000US","Nation":"United States","ID Year":2014,"Year":"2014","Population":314107084,"Slug Nation":"united-states"},{"ID Nation":"01000US","Nation":"United States","ID Year":2013,"Year":"2013","Population":311536594,"Slug Nation":"united-states"}],"source":[{"measures":["Population"],"annotations":{"source_name":"Census Bureau","source_description":"The American Community Survey (ACS) is conducted by the US Census and sent to a portion of the population every year.","dataset_name":"ACS 5-year Estimate","dataset_link":"http://www.census.gov/programs-surveys/acs/","table_id":"B01003","topic":"Diversity","subtopic":"Demographics"},"name":"acs_yg_total_population_5","substitutions":[]}]}

        // console.log('data:', data.data);


        const sqlSelect = `SELECT * FROM ${DATABASE_SCHEMA}.api_data`;
        const sqlQuerySelect = await db.query(sqlSelect);
        console.log('Selected data:', sqlQuerySelect);

        const populate = async () => {
                if (sqlQuerySelect.length > 0) {
                    return
                };
                data.data.map((item) => {
                    db[DATABASE_SCHEMA].api_data.insert({doc_record: item });
                });
            }
          
        populate();
   


        // a. em memoria no nodejs usando map, filter, for etc
        const filteredData = data.data.filter(item => item.Year === '2020' || item.Year === '2019' || item.Year === '2018');
        const totalPopulation = filteredData.reduce((acc, val) => acc + val.Population, 0);
        console.log('Total population:', totalPopulation);


        // Utilizando os HOFs para somar os valores
        // const hofResult = filteredData
        // .map((item) => item.Population)
        // .reduce((acc, val) => acc + val);
        // console.log('Sum using Hofs:', hofResult);

        // // Utilizando o for para somar os valores
        // let forResult = 0;
        // for (let i = 0; i < filteredData.length; i++) {
        //   forResult += filteredData[i].Population;
        // }
        // console.log('Sum using for', forResult); 

        // comentado para evitar ficar inserindo dados no banco
        // Adicionano os dados no banco

        

        // const sqlDelete = `DELETE FROM ${DATABASE_SCHEMA}.api_data where id = '1'`;
        // const sqlQueryDelete = await db.query(sqlDelete);
        // console.log('Deleted data:', sqlQueryDelete);

        // const sqlDelete = `DELETE FROM ${DATABASE_SCHEMA}.api_data`;
        // const sqlQueryDelete = await db.query(sqlDelete);
        // console.log('Deleted data:', sqlQueryDelete);
        
        // const sqlSelect = `SELECT * FROM ${DATABASE_SCHEMA}.api_data`;
        // const sqlQuerySelect = await db.query(sqlSelect);
        // console.log('Selected data:', sqlQuerySelect);

        
        // const sqlSumPopulation = `SELECT SUM(Population) AS total_population FROM populacao
        // WHERE (elem->>'Year')::int IN (2020, 2019, 2018);
        // `;
        const sqlSumPopulation = `SELECT SUM((doc_record->>'Population')::int) as total_population FROM ${DATABASE_SCHEMA}.api_data`;
        // WHERE (elem->>'Year')::int IN (2020, 2019, 2018);
        

        const sqlQuerySum = await db.query(sqlSumPopulation);
        console.log('Selected data:', sqlQuerySum);

        

        
        // const result = await db[DATABASE_SCHEMA].api_data.insert({ doc_record: data });
        // console.log('Inserted data:', result);
        

        // const sql = `SELECT doc_record FROM ${DATABASE_SCHEMA}.api_data where id = '18'`;
        // const result = await db.query(sql);
        // const dataResult = result[0].doc_record.data;

        // console.log(dataResult); // imprime o array de dados


        // const sumPopulationFromSql = `SELECT sum((elem->>'Population')::int) as total_population
        // FROM (
        //   SELECT jsonb_array_elements(doc_record->'data') as elem
        //   FROM ${DATABASE_SCHEMA}.api_data
        //   WHERE id = '18'
        // ) as subquery
        // WHERE (elem->>'Year')::int IN (2020, 2019, 2018);
        // `;

        // const resultBySql = await db.query(sumPopulationFromSql);
        // console.log('Total population in years 2020, 2019 and 2018 (using SELECT in PostgreSQL):', resultBySql);


        // // c. usando SELECT no postgres, pode fazer uma VIEW no banco de dados.
        // const view = `CREATE OR REPLACE VIEW population_sum AS
        // SELECT sum((elem->>'Population')::int) as total_population
        // FROM (
        //   SELECT jsonb_array_elements(doc_record->'data') as elem
        //   FROM ${DATABASE_SCHEMA}.api_data
        //   WHERE id = '18'
        // ) as subquery
        // WHERE (elem->>'Year')::int IN (2020, 2019, 2018);`

        // // const resultByView = await db.query(view);

        // const sqlQueryView = `SELECT * FROM population_sum;`;
        // const sqlQuerySelect = await db.query(sqlQueryView);

        // console.log('Total population in years 2020, 2019 and 2018 (using view in PostgreSQL):', sqlQuerySelect);

        // // b. usando SELECT no postgres, pode fazer um SELECT inline no nodejs.
        // db.query('SELECT * FROM population_sum;').then((data) => {
        //     console.log('data',  data);
        // }
        // );

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();