/**
 * Query the most recent betting lines 
**/

const YOUR_DATA_SHEET = "RawData";

function getNextWednesday1AM() {
  /** 
   * This function exists to resolve the current 
   * date and only get the football games for the 
   * current week. 
   * **/
    let now = new Date();
    let result = new Date(now);

    result.setHours(1, 0, 0, 0);

    let dayOfWeek = result.getDay();
    let daysUntilWed = (3 - dayOfWeek + 7) % 7;

    // If today is Wednesday and it's already past 1 AM, jump to next week
    if (daysUntilWed === 0 && now >= result) {
      daysUntilWed = 7;
    }

    result.setDate(result.getDate() + daysUntilWed);

    return [result.toISOString().split(".")[0] + "Z", now.toISOString().split(".")[0] + "Z"];
}

function getWeekNumberSinceLastTuesday() {
  let now = new Date();

  // Find most recent Tuesday at 00:00
  let lastTuesday = new Date(now);
  lastTuesday.setHours(0, 0, 0, 0);

  // JS: Sunday=0, Monday=1, Tuesday=2, ...
  let dayOfWeek = lastTuesday.getDay();
  let diff = (dayOfWeek - 2 + 7) % 7; // days since last Tuesday
  lastTuesday.setDate(lastTuesday.getDate() - diff);

  // Compute difference in weeks (floor division)
  let diffMs = now - lastTuesday;
  let weeks = Math.floor(diffMs / (7 * 24 * 60 * 60 * 1000));

  // Add 1 so current week = 1, next = 2, etc.
  return weeks + 1;
}

function main() {

    var dates = getNextWednesday1AM();
    let endDate = dates[0];
    let startDate = dates[1];
    console.log(startDate);

    // token needed to run api + ngrok URL for short TERM endpoint
    var apiToken = 'a1a6f44daa20a5d5eb8b2e12046dc4a2';
    var url = `https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds/?regions=us&markets=totals,spreads&oddsFormat=american&apiKey=${apiToken}&commenceTimeTo=${endDate}&commenceTimeFrom=${startDate}`; //api endpoint 
  
    var options = {
       "method" : "GET",
       "headers" : {
         "content-type":"application/json"
       },
       "payload":{},
       "muteHttpExceptions":true
     };


  
    function handleResponse(response){
      console.log(response.getContentText())
      var responseCode = response.getResponseCode();
    
      // switch for possible response codes
      switch (responseCode) {
        case 429:
          throw new Error('too many requests at once. wait a moment + re-enter formula');
        case 200:
          var json = response.getContentText(); // get the response content as text
          var mae = JSON.parse(json); //parse text into json
          var results = Object.values(mae);
          break;
        case 500:
          throw new Error(response.getContentText());
        default:
          var results = null;
          break;
      }
    return results;
  }

  function parseMatchups(data) {
    let results = [];

    data.forEach(match => {
      let home = match.home_team;
      let away = match.away_team;

      // Default values
      let spread = null;
      let overUnder = null;

      // Loop through bookmakers to extract spreads and totals
      match.bookmakers.forEach(bookmaker => {
        bookmaker.markets.forEach(market => {
          if (market.key === "spreads") {
            let homeSpread = market.outcomes.find(o => o.name === home);
            if (homeSpread) {
              spread = homeSpread.point;
            }
          }
          if (market.key === "totals") {
            let over = market.outcomes.find(o => o.name === "Over");
            if (over) {
              overUnder = over.point;
            }
          }
        });
      });

      // Push a row array (ready for Google Sheets)
      results.push([ getWeekNumberSinceLastTuesday(), home, away, spread, overUnder, new Date()]);
    });

    return results;
  }

  function appendDataToSheet(sheet, data) {
    // data should be a 2D array: [[row1col1, row1col2], [row2col1, row2col2], ...]
    data.forEach(row => {
      sheet.appendRow(row);
    });
  }

    var sheet = SpreadsheetApp.getActive().getSheetByName(YOUR_DATA_SHEET);

    // call api, log and return to sheet
    var response = UrlFetchApp.fetch(url, options); // get api endpoint
    var results = handleResponse(response);
    var parsed = parseMatchups(results);
    
    appendDataToSheet(sheet, parsed);
    return results;
}
  