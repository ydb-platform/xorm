-- Insert data into `/local/series`

UPSERT INTO `series` (`series_id`, `title`, `series_info`, `release_date`, `comment`) VALUES 
('8802e71f-d978-4379-8e7d-80090387c2c7',CAST('Silicon Valley' AS Optional<Utf8>),CAST('Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.' AS Optional<Utf8>),CAST(1396742400000000 AS Optional<Timestamp>),CAST('Some comment here' AS Optional<Utf8>)),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88',CAST('IT Crowd' AS Optional<Utf8>),CAST('The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by Ash Atalla and starring Chris O\'\'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.' AS Optional<Utf8>),CAST(1138924800000000 AS Optional<Timestamp>), '');

-- Insert data into `/local/seasons`

UPSERT INTO `seasons` (`series_id`, `season_id`, `title`, `first_aired`, `last_aired`) VALUES 
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171',CAST('Season 4' AS Optional<Utf8>),CAST(1277424000000000 AS Optional<Timestamp>),CAST(1280448000000000 AS Optional<Timestamp>)),
('8802e71f-d978-4379-8e7d-80090387c2c7','54a1f467-ef2e-454e-977a-a82145484319',CAST('Season 5' AS Optional<Utf8>),CAST(1521936000000000 AS Optional<Timestamp>),CAST(1526169600000000 AS Optional<Timestamp>)),
('8802e71f-d978-4379-8e7d-80090387c2c7','be84cb36-ac22-4346-a74e-73cde0c7393c',CAST('Season 1' AS Optional<Utf8>),CAST(1138924800000000 AS Optional<Timestamp>),CAST(1141344000000000 AS Optional<Timestamp>)),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','51efced2-8c7c-4be0-864f-d3fadfed3224',CAST('Season 1' AS Optional<Utf8>),CAST(1138924800000000 AS Optional<Timestamp>),CAST(1141344000000000 AS Optional<Timestamp>)),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6a916967-2be5-43e5-9356-c99c6233f950',CAST('Season 4' AS Optional<Utf8>),CAST(1277424000000000 AS Optional<Timestamp>),CAST(1280448000000000 AS Optional<Timestamp>));

-- Insert data into `/local/episodes`

UPSERT INTO `episodes` (`series_id`, `season_id`, `episode_id`, `title`, `air_date`, `views`) VALUES 
('8802e71f-d978-4379-8e7d-80090387c2c7','078601d7-5a23-429a-932b-5ad19e75b607','0a958d0a-920b-4745-9d79-d7e403e23903',CAST('White Hat/Black Hat' AS Optional<Utf8>),CAST(1433030400000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','078601d7-5a23-429a-932b-5ad19e75b607','1187c66e-db36-4f43-8f07-fd4f29384087',CAST('Homicide' AS Optional<Utf8>),CAST(1431820800000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','078601d7-5a23-429a-932b-5ad19e75b607','21d1b5b0-22c3-449d-bd88-9b5b5ed54a3f',CAST('test' AS Optional<Utf8>),CAST(1430611200000000 AS Optional<Timestamp>),CAST('999' AS Optional<Uint64>)),
('8802e71f-d978-4379-8e7d-80090387c2c7','078601d7-5a23-429a-932b-5ad19e75b607','41137bf8-815c-4dc6-abc0-b6f4b2ca785f',CAST('Two Days of the Condor' AS Optional<Utf8>),CAST(1434240000000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','078601d7-5a23-429a-932b-5ad19e75b607','493d04f0-dac8-42e2-bc1b-84dc9bb1e099',CAST('Runaway Devaluation' AS Optional<Utf8>),CAST(1429401600000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','078601d7-5a23-429a-932b-5ad19e75b607','56113b02-ed67-4ad1-ad87-5516c12582e9',CAST('Bad Money' AS Optional<Utf8>),CAST(1430006400000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','078601d7-5a23-429a-932b-5ad19e75b607','636c215d-bcdb-4571-a779-cedd07c703ca',CAST('Sand Hill Shuffle' AS Optional<Utf8>),CAST(1428796800000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','078601d7-5a23-429a-932b-5ad19e75b607','9b1432d0-850d-44bb-bc76-1d4fe0e64101',CAST('Server Space' AS Optional<Utf8>),CAST(1431216000000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','078601d7-5a23-429a-932b-5ad19e75b607','df6cdb08-2fc4-43bf-80b5-c970f752c7a5',CAST('Adult Content' AS Optional<Utf8>),CAST(1432425600000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','078601d7-5a23-429a-932b-5ad19e75b607','e02cd943-31c1-4185-bcbb-cf803be61dc7',CAST('Binding Arbitration' AS Optional<Utf8>),CAST(1433635200000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171','1589f366-2239-4c37-909b-797439094b53',CAST('Server Error' AS Optional<Utf8>),CAST(1498348800000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171','3385778f-0206-4f94-b80a-b5542a2bd657',CAST('Customer Service' AS Optional<Utf8>),CAST(1495929600000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171','3bde20f8-ba00-418d-8c6b-058110575879',CAST('Success Failure' AS Optional<Utf8>),CAST(1492905600000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171','6818d030-ee1f-4626-92fc-03965fcebce5',CAST('Terms of Service' AS Optional<Utf8>),CAST(1493510400000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171','6d6616bb-c98e-4f0c-a097-c9aad558dfd8',CAST('Hooli-Con' AS Optional<Utf8>),CAST(1497744000000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171','78005ee7-7ad6-4c58-aac3-cd6eb2871017',CAST('test' AS Optional<Utf8>),CAST(1497139200000000 AS Optional<Timestamp>),CAST('999' AS Optional<Uint64>)),
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171','7900f321-3907-4cc1-826a-69344359ca28',CAST('test' AS Optional<Utf8>),CAST(1496534400000000 AS Optional<Timestamp>),CAST('999' AS Optional<Uint64>)),
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171','7b1afb5f-5259-4aca-b54f-84d72b995748',CAST('Intellectual Property' AS Optional<Utf8>),CAST(1494115200000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171','83f046ff-6e1b-4f84-9db2-5a20527382a4',CAST('Teambuilding Exercise' AS Optional<Utf8>),CAST(1494720000000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','2b123989-6cf5-4e95-94cc-e5ec6eabd171','aef2d1e5-02c0-41ac-926b-fad43d6bc0d8',CAST('test' AS Optional<Utf8>),CAST(1495324800000000 AS Optional<Timestamp>),CAST('999' AS Optional<Uint64>)),
('8802e71f-d978-4379-8e7d-80090387c2c7','54a1f467-ef2e-454e-977a-a82145484319','0fe8e4a2-2b1a-4a61-95ec-b4a6432c038c',CAST('Initial Coin Offering' AS Optional<Utf8>),CAST(1525564800000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','54a1f467-ef2e-454e-977a-a82145484319','382b35eb-2615-4b58-afb1-b591d905135f',CAST('Grow Fast or Die Slow' AS Optional<Utf8>),CAST(1521936000000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','54a1f467-ef2e-454e-977a-a82145484319','7b8db0cb-1ba3-4329-abfe-bd053624dcb2',CAST('Tech Evangelist' AS Optional<Utf8>),CAST(1523750400000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','54a1f467-ef2e-454e-977a-a82145484319','8eb19cb1-c3cf-473b-9b88-248558422731',CAST('Facial Recognition' AS Optional<Utf8>),CAST(1524355200000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','54a1f467-ef2e-454e-977a-a82145484319','a8c01056-3ed5-4615-87fa-f0047ccf612d',CAST('Artificial Emotional Intelligence' AS Optional<Utf8>),CAST(1524960000000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','54a1f467-ef2e-454e-977a-a82145484319','e270ba41-ce67-4725-a192-6df9d97e6bdf',CAST('Reorientation' AS Optional<Utf8>),CAST(1522540800000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','54a1f467-ef2e-454e-977a-a82145484319','e67bc0f1-bc04-4079-be07-15dadddb560b',CAST('Chief Operating Officer' AS Optional<Utf8>),CAST(1523145600000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','54a1f467-ef2e-454e-977a-a82145484319','e913806f-16b8-4298-b7d2-533352b5659a',CAST('Fifty-One Percent' AS Optional<Utf8>),CAST(1526169600000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','57fd7b2c-2757-48f3-b445-4d7b237a1d1c','20745b9d-c9d8-443f-b27f-8f3633b076a6',CAST('Founder Friendly' AS Optional<Utf8>),CAST(1461456000000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','57fd7b2c-2757-48f3-b445-4d7b237a1d1c','59968ede-61ed-4547-af4a-9a1b26ece417',CAST('test' AS Optional<Utf8>),CAST(1463875200000000 AS Optional<Timestamp>),CAST('999' AS Optional<Uint64>)),
('8802e71f-d978-4379-8e7d-80090387c2c7','57fd7b2c-2757-48f3-b445-4d7b237a1d1c','5bfd22db-e967-46c2-91c5-5e1a79aeee17',CAST('Bachmanity Insanity' AS Optional<Utf8>),CAST(1464480000000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','57fd7b2c-2757-48f3-b445-4d7b237a1d1c','622d5664-8361-4a21-b90e-febc6920e8a7',CAST('Meinertzhagen\'\'s Haversack' AS Optional<Utf8>),CAST(1462665600000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','57fd7b2c-2757-48f3-b445-4d7b237a1d1c','6717a4d1-1510-4cda-bf1b-bd428f01078a',CAST('Maleant Data Systems Solutions' AS Optional<Utf8>),CAST(1463270400000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','57fd7b2c-2757-48f3-b445-4d7b237a1d1c','7fc1149e-78d7-47b4-88ff-cca0337e06fe',CAST('To Build a Better Beta' AS Optional<Utf8>),CAST(1465084800000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','57fd7b2c-2757-48f3-b445-4d7b237a1d1c','c93e9ff1-c9e2-4b21-9c08-23993b5de66c',CAST('test' AS Optional<Utf8>),CAST(1466899200000000 AS Optional<Timestamp>),CAST('999' AS Optional<Uint64>)),
('8802e71f-d978-4379-8e7d-80090387c2c7','57fd7b2c-2757-48f3-b445-4d7b237a1d1c','e5d88eb6-25e0-451d-a558-f8a7821e0e33',CAST('Two in the Box' AS Optional<Utf8>),CAST(1462060800000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','57fd7b2c-2757-48f3-b445-4d7b237a1d1c','f3f4b8bb-0ebd-4c2d-aab2-c6e3152fbdab',CAST('Bachman\'\'s Earnings Over-Ride' AS Optional<Utf8>),CAST(1465689600000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','57fd7b2c-2757-48f3-b445-4d7b237a1d1c','fbdf6b46-417a-4978-bbe8-146cdb235a6d',CAST('Daily Active Users' AS Optional<Utf8>),CAST(1466294400000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','be84cb36-ac22-4346-a74e-73cde0c7393c','49548f34-319f-4c25-a0ea-c0eaf795bd8c',CAST('Fiduciary Duties' AS Optional<Utf8>),CAST(1398556800000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','be84cb36-ac22-4346-a74e-73cde0c7393c','527f28f6-f865-48fc-b22a-d3c69d168408',CAST('Proof of Concept' AS Optional<Utf8>),CAST(1400371200000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','be84cb36-ac22-4346-a74e-73cde0c7393c','5ef1805b-0cdf-451a-9831-e039931a6203',CAST('Signaling Risk' AS Optional<Utf8>),CAST(1399161600000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','be84cb36-ac22-4346-a74e-73cde0c7393c','71fb225c-327a-430c-ba57-244b49404dd5',CAST('Optimal Tip-to-Tip Efficiency' AS Optional<Utf8>),CAST(1401580800000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','be84cb36-ac22-4346-a74e-73cde0c7393c','a955e7be-43da-4ba4-ac71-5dad86ecccd5',CAST('Articles of Incorporation' AS Optional<Utf8>),CAST(1397952000000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','be84cb36-ac22-4346-a74e-73cde0c7393c','bf2ba43c-9542-41a3-994c-6edf96f5138e',CAST('test' AS Optional<Utf8>),CAST(1397347200000000 AS Optional<Timestamp>),CAST('999' AS Optional<Uint64>)),
('8802e71f-d978-4379-8e7d-80090387c2c7','be84cb36-ac22-4346-a74e-73cde0c7393c','d56fcf97-c7bb-4c96-a51b-b35363525dc9',CAST('Minimum Viable Product' AS Optional<Utf8>),CAST(1396742400000000 AS Optional<Timestamp>),NULL),
('8802e71f-d978-4379-8e7d-80090387c2c7','be84cb36-ac22-4346-a74e-73cde0c7393c','e181a2f2-1ffa-4283-b5b1-0226f3805a2c',CAST('Third Party Insourcing' AS Optional<Utf8>),CAST(1399766400000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','51efced2-8c7c-4be0-864f-d3fadfed3224','47118ec6-d0e2-4164-9876-415512cd32ef',CAST('Yesterday\'\'s Jam' AS Optional<Utf8>),CAST(1138924800000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','51efced2-8c7c-4be0-864f-d3fadfed3224','4c72e475-2003-4e7f-a7e7-5c2c27512fc8',CAST('The Red Door' AS Optional<Utf8>),CAST(1140134400000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','51efced2-8c7c-4be0-864f-d3fadfed3224','98c194f8-622d-4f5c-9e6d-653bcf6cb157',CAST('Aunt Irma Visits' AS Optional<Utf8>),CAST(1141344000000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','51efced2-8c7c-4be0-864f-d3fadfed3224','9fa9aeb5-347a-4888-b451-c6edaad9ecc0',CAST('The Haunting of Bill Crouse' AS Optional<Utf8>),CAST(1140739200000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','51efced2-8c7c-4be0-864f-d3fadfed3224','b46d5977-6891-46ad-878c-7ef9c4e10ad3',CAST('Fifty-Fifty' AS Optional<Utf8>),CAST(1139529600000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','51efced2-8c7c-4be0-864f-d3fadfed3224','de57626f-5b43-4673-bc9c-a0d1fef2fb20',CAST('Calamity Jen' AS Optional<Utf8>),CAST(1138924800000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6111aefa-f8de-4b2e-a1a1-d96b31ae3449','158a5818-f853-499c-aa1f-9088c0229054',CAST('Calendar Geeks' AS Optional<Utf8>),CAST(1230249600000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6111aefa-f8de-4b2e-a1a1-d96b31ae3449','19f4c4f4-4018-4139-9c48-02e491560f79',CAST('Friendface' AS Optional<Utf8>),CAST(1229644800000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6111aefa-f8de-4b2e-a1a1-d96b31ae3449','71eda16f-10b8-42c7-ac6e-fbf5ad61c049',CAST('Are We Not Men?' AS Optional<Utf8>),CAST(1227830400000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6111aefa-f8de-4b2e-a1a1-d96b31ae3449','7d558a59-e17f-481e-8b3b-03b54cf6a635',CAST('Tramps Like Us' AS Optional<Utf8>),CAST(1228435200000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6111aefa-f8de-4b2e-a1a1-d96b31ae3449','80541d00-ca6d-4380-87bb-619894dd1997',CAST('From Hell' AS Optional<Utf8>),CAST(1227225600000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6111aefa-f8de-4b2e-a1a1-d96b31ae3449','eaf80f6b-8b1e-4f04-8da2-070791afeba4',CAST('The Speech' AS Optional<Utf8>),CAST(1229040000000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6a916967-2be5-43e5-9356-c99c6233f950','2c1776bf-8861-4728-8a71-2d62ccd608ef',CAST('Reynholm vs Reynholm' AS Optional<Utf8>),CAST(1280448000000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6a916967-2be5-43e5-9356-c99c6233f950','46b4b4c8-0ce7-48b2-912e-a964161e8ec3',CAST('Bad Boys' AS Optional<Utf8>),CAST(1279843200000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6a916967-2be5-43e5-9356-c99c6233f950','584360cf-8ff3-47ba-b44e-4c026e545f3d',CAST('Something Happened' AS Optional<Utf8>),CAST(1278633600000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6a916967-2be5-43e5-9356-c99c6233f950','96d6fda1-84f6-4c56-8763-9604b51faa11',CAST('The Final Countdown' AS Optional<Utf8>),CAST(1278028800000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6a916967-2be5-43e5-9356-c99c6233f950','9c73e47e-1e20-49a0-9285-7b08d0fd1b06',CAST('Jen The Fredo' AS Optional<Utf8>),CAST(1277424000000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','6a916967-2be5-43e5-9356-c99c6233f950','b123a783-ddfe-42b7-9593-c3160d68db68',CAST('Italian For Beginners' AS Optional<Utf8>),CAST(1279238400000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','b5b5d9f6-82d4-4405-81e1-596213d50356','169bce9d-70ca-4c3e-8cd0-c11dd704feca',CAST('Smoke and Mirrors' AS Optional<Utf8>),CAST(1190332800000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','b5b5d9f6-82d4-4405-81e1-596213d50356','2ff32dfb-1b2f-48cb-8366-2ddfdadbea69',CAST('The Work Outing' AS Optional<Utf8>),CAST(1156377600000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','b5b5d9f6-82d4-4405-81e1-596213d50356','4d59dbf3-53cb-4b4e-ba8c-a04b8679afcc',CAST('Moss and the German' AS Optional<Utf8>),CAST(1189123200000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','b5b5d9f6-82d4-4405-81e1-596213d50356','ad69fc17-9aff-4c80-b65e-06553cd7375a',CAST('Men Without Women' AS Optional<Utf8>),CAST(1190937600000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','b5b5d9f6-82d4-4405-81e1-596213d50356','af704207-8bba-4fdc-8bf0-331c8cdd8d4e',CAST('Return of the Golden Child' AS Optional<Utf8>),CAST(1188518400000000 AS Optional<Timestamp>),NULL),
('e83bf413-6555-4db8-a71d-9d39c2e5cb88','b5b5d9f6-82d4-4405-81e1-596213d50356','d6f6b37f-a1f2-490f-a142-a9d8ecb65014',CAST('The Dinner Party' AS Optional<Utf8>),CAST(1189728000000000 AS Optional<Timestamp>),NULL);
