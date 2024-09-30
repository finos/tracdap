/**
 * A group of utilities for processing numbers.
 * @category Utils
 * @module NumberUtils
 */

import secondsToHours from "date-fns/secondsToHours";
import secondsToMinutes from "date-fns/secondsToMinutes";
import {sOrNot} from "./utils_general";

/**
 * A function that takes a file size in bytes and converts it to a humanreadable version e.g. 1.7Mb. There is some additional logic
 * to remove strings ending in '.0' so 1000.0Mb becomes 1000Mb.
 *
 * @param sizeInBytes - The size in bytes.
 * @returns The humanreadable version.
 */
export const humanReadableFileSize = (sizeInBytes: number): string => {

    const i = sizeInBytes === 0 ? 0 : Math.floor(Math.log(sizeInBytes) / Math.log(1024))
    return `${(sizeInBytes / Math.pow(1024, i)).toFixed(0).replace(/\.0$/, "0")}${[" bytes", "kb", "Mb", "Gb", "Tb"][i]}`
}

/**
 * A function that rounds a number to a set number of decimal places.
 *
 * @see https://stackoverflow.com/questions/11832914/how-to-round-to-at-most-2-decimal-places-if-necessary
 *
 * @remarks
 * Rounding floating point numbers
 * is a rather large topic that is well covered on the internet so care needs to be taken to make sure
 * that this function works as expected.
 *
 * @param myNumber The number to round.
 * @param dec The number of decimal places to round the number to.
 */
export function roundNumberToNDecimalPlaces(myNumber: number, dec: number): number {

    // Use parseFloat instead of Number - tighter checking
    return myNumber === 0 ? 0 : +(Math.round(parseFloat(myNumber + "e+" + dec)) + "e-" + dec)
}

/**
 * A function that converts a UTC epoch to a datetime. UTC epoch is the number of seconds in a Date object since 1st Jan 1970.
 * @param utcMilliSeconds - The UTC epoch milliseconds to convert
 * @returns A Javascript date corresponding to the UtcEpoch.
 */
export const convertUtcEpochToDateTimeObject = (utcMilliSeconds: number): Date => {

    let d = new Date(0);
    d.setUTCMilliseconds(utcMilliSeconds);
    return d
}

/**
 * A function that takes a duration as a number of seconds and converts it into a human-readable string of the
 * duration e.g. 1 hr 5 min 56 sec. It is capable of returning a short form or long form version of the units.
 *
 * @param asSeconds - The duration in seconds.
 * @param longForm - Whether to use long form time units.
 * @returns The duration as a string.
 */
export const convertSecondsToHrMinSec = (asSeconds: number, longForm: boolean = false): string => {

    // Hours, minutes and seconds
    let newSeconds = Math.round(asSeconds)
    const hours = secondsToHours(newSeconds);
    newSeconds = newSeconds - hours * 60 * 60
    const minutes = secondsToMinutes(newSeconds);
    newSeconds = newSeconds - minutes * 60
    const seconds = newSeconds;

    const hourText = longForm ? "hour" : "hr"
    const minText = longForm ? "minute" : "min"
    const secText = longForm ? "second" : "sec"

    // Create the string to return
    let duration = ""
    let delimiter = ""
    if (hours > 0) {
        duration += `${hours} ${hourText}${sOrNot(hours)}`;
    }
    if (minutes > 0) {
        delimiter = hours === 0 ? "" : " "
        duration += `${delimiter}${minutes} ${minText}${sOrNot(minutes)}`;
    }
    if (seconds > 0 || (hours === 0 && minutes === 0)) {
        delimiter = hours === 0 && minutes === 0 ? "" : " "
        duration += `${delimiter}${seconds} ${secText}${sOrNot(seconds)}`;
    }

    return duration
}

/**
 * A function that takes a duration as a number of milliseconds and converts it into a human-readable string of the
 * duration e.g. 1 hr 5 min 56 sec. It is capable of returning a short form or long form version of the units.
 *
 * @param asMilliSeconds - The duration in seconds.
 * @param longForm - Whether to use long form time units.
 * @returns The duration as a string.
 */
export const convertMilliSecondsToHrMinSec = (asMilliSeconds: number, longForm: boolean = false): string => {

    return convertSecondsToHrMinSec(asMilliSeconds/1000, longForm)
}