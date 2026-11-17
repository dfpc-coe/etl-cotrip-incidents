import moment from 'moment-timezone';
import { Feature } from '@tak-ps/node-cot'
import type { Geometry, GeometryCollection, Point, LineString, Polygon } from 'geojson';
import { Static, Type, TSchema } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';

const InputSchema = Type.Object({
    'COTRIP_TOKEN': Type.String({ description: 'API Token for CoTrip' }),
    'Point Geometries': Type.Boolean({ description: 'Allow point geometries', default: true }),
    'LineString Geometries': Type.Boolean({ description: 'Allow LineString geometries', default: true }),
    'Polygon Geometries': Type.Boolean({ description: 'Allow Polygon Geometries', default: true }),
    'DEBUG': Type.Boolean({ description: 'Print GeoJSON Features in logs', default: false, })
});

const IncidentResponse = Type.Object({
    features: Type.Array(Type.Unknown())
});

const GeometryTypes = new Set([ 'Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', 'MultiPolygon' ]);

type IncidentGeometry = Exclude<Geometry, GeometryCollection>;

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

function isGeometry(value: unknown): value is IncidentGeometry {
    return isObject(value)
        && typeof value.type === 'string'
        && GeometryTypes.has(value.type)
        && Array.isArray(value.coordinates);
}

function splitGeometry(geometry: IncidentGeometry): Array<Point | LineString | Polygon> {
    if (geometry.type === 'MultiPoint') {
        return geometry.coordinates.map((coordinates) => ({ type: 'Point', coordinates }));
    }

    if (geometry.type === 'MultiLineString') {
        return geometry.coordinates.map((coordinates) => ({ type: 'LineString', coordinates }));
    }

    if (geometry.type === 'MultiPolygon') {
        return geometry.coordinates.map((coordinates) => ({ type: 'Polygon', coordinates }));
    }

    return [ geometry ];
}

function asString(value: unknown): string | undefined {
    if (typeof value === 'string') return value;
    if (typeof value === 'number') return String(value);
    return undefined;
}

function formatTime(value: unknown): string {
    const input = typeof value === 'string' || typeof value === 'number' || value === null ? value : undefined;
    return moment(input).tz('America/Denver').format('YYYY-MM-DD HH:mm z');
}

export default class Task extends ETL {
    static name = 'etl-cotrip-incidents';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow !== DataFlowType.Incoming) return Type.Object({});
        if (type === SchemaType.Input) return InputSchema;

        return Type.Object({
            incident_type: Type.String(),
            status: Type.String(),
            direction: Type.Number(),
            routeName: Type.String(),
            severity: Type.String(),
            responseLevel: Type.String(),
            category: Type.String(),
            startTime: Type.String(),
            startMarker: Type.Optional(Type.Number()),
            endMarker: Type.Optional(Type.Number()),
            lastUpdated: Type.String(),
            travelerInformationMessage: Type.String(),
        });
    }

    async control(): Promise<void> {
        const env = await this.env(InputSchema);

        const api = 'https://data.cotrip.org/';
        if (!env.COTRIP_TOKEN) throw new Error('No COTrip API Token Provided');
        const token = env.COTRIP_TOKEN;

        const incidents: unknown[] = [];
        let batch = -1;
        let res;
        do {
            console.log(`ok - fetching ${++batch} of incidents`);
            const url = new URL('/api/v1/incidents', api);
            url.searchParams.append('apiKey', token);
            if (res) {
                const nextOffset = res.headers.get('next-offset');
                if (nextOffset) url.searchParams.append('offset', nextOffset);
            }

            res = await fetch(url);

            const body: unknown = await res.json();
            if (!Value.Check(IncidentResponse, body)) throw new Error('Unexpected Incidents Response');

            incidents.push(...body.features);
        } while (res.headers.has('next-offset') && res.headers.get('next-offset') !== 'None');
        console.log(`ok - fetched ${incidents.length} incidents`);

        const features: Static<typeof Feature.InputFeature>[] = [];
        for (const incident of incidents) {
            if (!isObject(incident) || !isObject(incident.properties)) continue;

            const props = incident.properties;
            if (!isObject(incident.geometry)) throw new Error('Incident has no geometry');

            const geometry = incident.geometry;
            if (!isGeometry(geometry)) continue;

            const id = asString(props.id);
            const multi = geometry.type.startsWith('Multi');

            splitGeometry(geometry).forEach((part, i) => {
                features.push({
                    id: multi && id !== undefined ? `${id}-${i}` : id,
                    type: 'Feature',
                    properties: {
                        remarks: asString(props.travelerInformationMessage),
                        callsign: asString(props.type),
                        type: geometry.type.endsWith('Point') ? 'a-f-G' : 'u-d-f',
                        metadata: {
                            incident_type: props.type,
                            status: props.status,
                            direction: props.direction,
                            routeName: props.routeName,
                            severity: props.severity,
                            responseLevel: props.responseLevel,
                            category: props.category,
                            startMarker: props.startMarker,
                            endMarker: props.endMarker,
                            startTime: formatTime(props.startTime),
                            lastUpdated: formatTime(props.lastUpdated),
                            travelerInformationMessage: props.travelerInformationMessage
                        }
                    },
                    geometry: part
                });
            });
        }

        const allowed: string[] = [];
        if (env['Point Geometries']) allowed.push('Point');
        if (env['LineString Geometries']) allowed.push('LineString');
        if (env['Polygon Geometries']) allowed.push('Polygon');

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features.filter((feat) => {
                return allowed.includes(feat.geometry.type);
            })
        };

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}): ReturnType<typeof internal> {
    return internal(await Task.init(import.meta.url), event);
}
