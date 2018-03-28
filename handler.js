const Kraken = require('kraken-api');
const Bitstamp = require('bitstamp');
const Promise = require('bluebird');
const Aws = require('aws-sdk');
const {format} = require('util');

// Instantiate clients
const kraken = new Kraken(process.env.KRAKEN_KEY, process.env.KRAKEN_SECRET, {
  timeout: 60 * 60 * 48 * 1000
});
const bitstamp = new Bitstamp(process.env.BITSTAMP_KEY, process.env.BITSTAMP_SECRET, process.env.BITSTAMP_CLIENT_ID, 60 * 60);
const sns = new Aws.SNS({region: process.env.SNS_REGION});

// Promisify callbacks
const asyncBitstampTicker = Promise.promisify(bitstamp.ticker);
const asyncBitstampBuy = Promise.promisify(bitstamp.buyMarket);
const asyncBitstampSell = Promise.promisify(bitstamp.sellMarket);

export const watch = async (event, context, callback) => {
  try {
    const pair = process.env.ASSET;
    const {bitstampPrice, krakenPrice} = await getPrices(pair);
    const spread = calculateSpread(bitstampPrice, krakenPrice);
    if (spread < process.env.SPREAD_THRESHOLD) {
      return callback(null, response);
    }
    if (krakenPrice < bitstampPrice) {
      // buy on kraken -> sell on bitstamp
      const krakenVolume = (( 1 / krakenPrice) * parseInt(process.env.AMOUNT)).toFixed(6);
      console.log(format('buying 100EUR of btc %f at %f on kraken', krakenVolume, krakenPrice));
      console.log(format('selling %f of btc %f at %f on bitstamp', (bitstampPrice*krakenVolume).toFixed(2), krakenVolume, bitstampPrice));
      // Try to buy on kraken
      try {
        const krakenResponse = await kraken.api('AddOrder', {
            pair: formatToKrakenPair(pair),
            volume: krakenVolume,
            type: 'buy',
            ordertype: 'market'
        });
        console.log(krakenResponse);
        const txIds = krakenResponse['result']['txid'];
        if (typeof txId === 'undefined') {
            return callback(false, {statusCode: 400, message: 'Unable to read kraken transaction id'});
        }
      } catch (e) {
        return callback(false, {statusCode: 400, message: JSON.stringify(e)});
      }
      // Now try to sell on bitstamp
      try {
        bitstampResponse = await asyncBitstampSell(pair.toLowerCase(), krakenVolume);
        console.log(bitstampResponse);
        const orderId = bitstampResponse['id'];
        if (typeof orderId === 'undefined') {
            // send a message that we fucked up, we bought but not sold, or add a retry
            return callback(false, {statusCode: 400, message: 'Unable to read bitstamp order id'});
        }
      } catch (e) {
        // send a message that we fucked up, we bought but not sold, or add a retry
        return callback(false, {statusCode: 400, message: JSON.stringify(e)});
      }
      const message = format('completed successfully bought %fEUR and sold %fEUR', parseInt(process.env.AMOUNT), (bitstampPrice*krakenVolume).toFixed(2));
      await sendMeAMessagePlease(message);
      return callback(null, {statusCode: 201, message: message});
    } else {
      // buy {amount} on bitstamp -> sell on kraken
      const bitstampVolume = (( 1 / bitstampPrice) * parseInt(process.env.AMOUNT)).toFixed(6);
      console.log(format('buying 100EUR of btc %f at %f on bitstamp', bitstampVolume, bitstampPrice));
      console.log(format('selling %fEUR of btc %f at %f on kraken', (krakenPrice*bitstampVolume).toFixed(2), bitstampVolume, krakenPrice));
      // Try to buy on bitstamp
      try {
        const bitstampResponse = await asyncBitstampBuy(pair.toLowerCase(), bitstampVolume);
        console.log(bitstampResponse);
        const orderId = bitstampResponse['id'];
        if (typeof orderId === 'undefined') {
            return callback(false, {statusCode: 400, message: 'Unable to read bitstamp order id'});
        }
      } catch (e) {
        return callback(false, {statusCode: 400, message: JSON.stringify(e)});
      }
      // Try to sell on kraken
      try { 
        const krakenResponse = await kraken.api('AddOrder', {
          pair: pair,
          volume: bitstampVolume,
          type: 'sell',
          ordertype: 'market'
        });
        console.log(krakenResponse);
        const txIds = krakenResponse['result']['txid'];
        if (typeof txId === 'undefined') {
            // send a message that we fucked up, we bought but not sold, or add a retry
            return callback(false, {statusCode: 400, message: 'Unable to read kraken transaction id'});
        }
      } catch (e) {
        // send a message that we fucked up, we bought but not sold, or add a retry
        return callback(false, {statusCode: 400, message: JSON.stringify(e)});
      }
      const message = format('completed successfully bought %fEUR and sold %fEUR', parseInt(process.env.AMOUNT), (krakenPrice*bitstampVolume).toFixed(2));
      await sendMeAMessagePlease(message);
      return callback(null, {statusCode: 201, message: message});
    }
  } catch (e) {
    console.log(e);
    callback(null, {statusCode: 401, message: JSON.stringify(e)});
  }
};

// send SMS notification
export const sendMeAMessagePlease = async message => {
  return await sns.publish({
    TargetArn: process.env.SNS_TOPIC_ARN,
    Subject: 'BTCINVEST',
    Message: message
  }).promise();
};

// returned in percentage points
export const calculateSpread = (krakenPrice, bitstampPrice) => {
  // get lower / higher price
  const {lower, higher} = minMax(krakenPrice, bitstampPrice);

  return (100 - ((lower / higher) * 100)).toFixed(2);
};

// given a currency pair returns the current ask price in the two exchanges
export const getPrices = async pair => {
  const krakenPromise = kraken.api('Ticker', { pair : formatToKrakenPair(pair) });
  const bitstampPromise = asyncBitstampTicker(pair);
  // Get Ticker Info
  const [krakenResponse, bitstampResponse] = await Promise.all([krakenPromise, bitstampPromise])
  const krakenPrice = krakenResponse['result'][formatToKrakenPair(pair)]['a'][0];
  const bitstampPrice = bitstampResponse['ask'];

  return {bitstampPrice, krakenPrice};
};

// kraken uses a special syntax for ticker pair
// this is horrible dont judge me
export const formatToKrakenPair = pair => {
  let first = pair.substring(0, 3);
  let second = pair.substring(3);
  let krakenPair = '';
  if (isFiatCurrency(first)) {
      krakenPair += 'Z'+first;
  } else {
      krakenPair += 'X'+formatToKrakenXBT(first);
  }
  if (isFiatCurrency(second)) {
      krakenPair += 'Z'+second;
  } else {
      krakenPair += 'X'+formatToKrakenXBT(second);
  }

  return krakenPair;
};

// checks whether is a fiat currency
export const isFiatCurrency = c => {
  return ['USD', 'EUR', 'JPY', 'GBP', 'CAD'].indexOf(c) !== -1;
};

// kraken uses the currency code XBT instead of BTC for bitcoin
export const formatToKrakenXBT = btc => {
  return btc === 'BTC' ? 'XBT' : btc;
};

// returns whether of the values are higher or lower
export const minMax = (a, b) => {
    let lower = a;
    let higher = b;
    if (lower > b) {
        lower = b;
        higher = a;
    }
    return {lower, higher};
};
