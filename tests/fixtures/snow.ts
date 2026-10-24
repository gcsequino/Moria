import type { JsonLoader } from '../../services/snotel/src/connector.js';
export const snowConfig = {
  stations: ['663:CO:SNTL'],
  beginDate: '2025-04-01',
  endDate: '2025-04-03',
};
export function snowFixture(value = 12): JsonLoader {
  return async (url) =>
    url.pathname.endsWith('/stations')
      ? [
          {
            stationTriplet: '663:CO:SNTL',
            name: 'Niwot',
            stateCode: 'CO',
            latitude: 40,
            longitude: -105,
            elevation: 9940,
            dataTimeZone: -8,
            countyName: 'Boulder',
          },
        ]
      : [
          {
            stationTriplet: '663:CO:SNTL',
            data: ['SNWD', 'WTEQ'].map((elementCode) => ({
              stationElement: {
                elementCode,
                ordinal: 1,
                durationName: 'DAILY',
                storedUnitCode: 'in',
              },
              values: [
                { date: '2025-04-01', value, qcFlag: 'V', qaFlag: 'A' },
                { date: '2025-04-03', value: 0 },
              ],
            })),
          },
        ];
}

const reportHeader =
  '# Reporting Frequency: Daily\n# Report Instantaneous Data As: End of Day\nDate,Station Name,Station Id,State Code,Network Code,Latitude,Longitude,Elevation (ft),Snow Water Equivalent (in) End of Day Values,Snow Depth (in) End of Day Values\n';
export const reportFixture =
  reportHeader +
  '2025-04-01,"Niwot, test",663,CO,SNTL,40,-105,9940,12.6,37\n2025-04-03,"Niwot, test",663,CO,SNTL,40,-105,9940,0,\n';
