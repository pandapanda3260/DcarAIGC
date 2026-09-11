"""One exact schema21 classification successor of the account-cleanup generation.

The original install, activation, RELEASE and operator scopes remain immutable.
A new loaded-code receipt proves an explicitly reviewed source delta; it does
not create or alter capture authority. Empty review pins reject preparation.
This module is stdlib-only so the fully hashed bootstrap can import it safely.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Any, Mapping

CONTRACT = "account-classification-schema-successor-v1"
TRANSITION = "account-classification-20260908-v1"
MODULE = "src/dcar_eval/v8/account_classification_release.py"
PARENT_BUILD_SHA256 = "55fb441ead8f76a88ca75cd65d7ef814b3280a5fb3c5fe70350a6c8889736671"
PARENT_INSTALL_SHA256 = "4d82746e8373905dddf8f4c3a8dd7c687635a71cbc7d9e53120da396ef104d39"
APPROVED_PREDECESSOR_BUILD_SHA256 = frozenset({PARENT_BUILD_SHA256,
    "2ab8de5c2137f9a71b8679a22f27c35c48519df596c10438f5509dd3b53cc4db"})
# Fill only after the final source delta has been independently reviewed.
# This module's own hash is bound through the loaded file and full source tree.
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'app/web/app/accounts/AccountsPage.tsx': {'before_sha256': '21255af552e83b0e9e8d05a33ad03348d45f8dc15addea8df766ee90625d9377', 'after_sha256': '9d448c8e1a561cd5d0cdc33d97c28672f5bad8feb03f281b7378946ea121c3f5'}, 'app/web/app/accounts/CreateAccountDialog.tsx': {'before_sha256': '7a85a3f5dab77422233f5340401ae082c5f47a0ed75f64e43cbaacef4b9bee53', 'after_sha256': '1740694a91fb41e1d6783246d9dffc41df9e4842448ce1006859efca2f3617ed'}, 'app/web/app/accounts/accounts.module.css': {'before_sha256': '2ab101074dfe0882729ee6a98ab196cf807c3468c5099581cc730f427abdd32e', 'after_sha256': '6829d49e4c2920f97992a9f925a02811f7afc4168f8b8af1db8772148db6dc23'}, 'app/web/app/accounts/douyin-authorization/DouyinAuthorizationPage.tsx': {'before_sha256': '958b5bdc95fc707722f20cefd87526132d6fc7134675baf3775b71a4f0b328f9', 'after_sha256': '6564004acdeff0eec3990d9e27c5d3121c727a682d9a99e1cf4bade7e919c3f1'}, 'app/web/app/contents/ContentsPage.tsx': {'before_sha256': '047a0f61bacd2080e5be33e1176ee444c71004ef7a256396431232abcbd94115', 'after_sha256': '008377c14bda88276ef441ecda12ccbd669e78084f7383f430ad676f59745349'}, 'app/web/app/contents/contentForm.ts': {'before_sha256': '9c68ddeb4bb15b14bcf083b152b8e066e69af9b5798ddc11e28176ff36814122', 'after_sha256': '572e95436f2adaf391c0eced70fd03795e98e517c67e185e3e3a512dd9429f1c'}, 'app/web/app/lib/accountClassification.ts': {'before_sha256': None, 'after_sha256': '4caabbcfaa170c28a8f0082d58274a123df2650aa24326b6e4f2df2d185eab6d'}, 'app/web/app/lib/queries.ts': {'before_sha256': 'c0cb9a9a19080375abff3e4b6ff547c6148135cfd5a3eb4a0222c6466f9ea348', 'after_sha256': '1b3fdbe7f2513ebfc6ba9960672c3f36a7fabe999ff66ad06b748e02dd497f84'}, 'app/web/app/lib/queryContracts.ts': {'before_sha256': '69d19aad66f0a94a67c9b7e9eac2eb3ce3ab3a985e0a75d0c4eb69adb9c8e19d', 'after_sha256': '1489c26cb96fadac05a1a5067c134ddbb8c789341f92cf9d9850d2ccd4e05db4'}, 'app/web/app/lib/types.ts': {'before_sha256': '055cb71882d59df5906d1228cb3bb7770c55c2b5388de55fa693398b5432b831', 'after_sha256': 'a1bd924584d7cfc1ae59ea8e644dc3638a624a727e6f25e6558c741eb08570e4'}, 'app/web/app/tasks/[id]/TaskDetailPage.tsx': {'before_sha256': 'ce131c4b6f0aced86c5d5737e4fe6fee13d74f64a89fab76b7a2524b56f7405c', 'after_sha256': '469cd6deab16e2e34a0c25796abeeb3ef3bb06e12bd4adf8361709534bda83e9'}, 'app/web/tests/account-access.test.mjs': {'before_sha256': 'c3778bbee333c927bfef0c584255e42e17e807bcda79fe8fe572a0c99bcd655d', 'after_sha256': '047df457082539dc9c2a23ab144c4213c1c89a9beab1b1268e3eaa3514b0936d'}, 'app/web/tests/account-create.test.mjs': {'before_sha256': '7b2c3a3c8cb2d5fbe4af0e47a88914110c91d8a95d25054ba43b506e0d3d4895', 'after_sha256': '09e4ab36854af310388e7bdfc96e9b24fee4445ec044fdf22a6836b2ed57b48c'}, 'app/web/tests/query-cache.test.mjs': {'before_sha256': '556145ecf377f9afa8b8af81cdebba692da6c4b444024e1c04a0d55b6f83c838', 'after_sha256': '93213c4da0cb1c673cf41a72d63841a3c270d5431771e9216aa076ca5f68f6e2'}, 'app/web/tests/rendered-html.test.mjs': {'before_sha256': 'de632da8c18e4e8302530c074ec06b41ad1a29609ee8bc4273c2e0e1037decef', 'after_sha256': '87f43c0f602f60d85f528bd3b853fba1692fb77718614ef00e440776020ec7f4'}, 'deploy/macos/publish_snapshot.py': {'before_sha256': '4465f8e8af68f6413d9e022e6b3834f0720928672be7dce78ea2b5a0a09a9d91', 'after_sha256': 'f80a0bb7d5fc88cba7b6bc2e9809c1a6ac3d3de96f2065dfe97ca6c75132a923'}, 'deploy/macos/run_snapshot_publisher.sh': {'before_sha256': '9e892a04697330279d7ec08376dcc3205b34e65076d8a7d2aa130b4cc708c0f4', 'after_sha256': '9fcb86ac4bdd15bc46d6fe208534a9eb31d92b837be3aea3e9152e8b922c4076'}, 'deploy/server/install_snapshot.py': {'before_sha256': 'cf3da94e133205091172b0cf32a82a20af7e65a82ce98a048a91edb7481a5e99', 'after_sha256': 'cc70ce7791ec84f8c1cb7a82a26efab0696f78c0d73db7643a589fe047c5c006'}, 'scripts/build_server_snapshot.py': {'before_sha256': 'bbdc9ab5cd5932f197e786e0011293e227399321efd93a01045084dd755e8d5d', 'after_sha256': 'ea0c8f64c277afbf72e725a8fa47be57b5837f16a301cf1c010fc2b873e7dbad'}, 'scripts/check_account_classification.py': {'before_sha256': None, 'after_sha256': 'e4cf20c39f8fdf84b373458eddd829327c206cec4ab1188838cb28121c350b8f'}, 'scripts/install_account_classification.py': {'before_sha256': None, 'after_sha256': '9b5b2ccfcdd0bffbbfe1f104d049141595177f9528634096b116a7e3d11b1dbd'}, 'scripts/prepare_account_classification_release.py': {'before_sha256': None, 'after_sha256': '05cdd3a0e28f008bcccce11613676bf39df41b9b0c23d9aca7ab49dff81cee89'}, 'scripts/v20_release_contract.py': {'before_sha256': '540d3313a56075d8375c435e8fc30d3b938f40d266f63878747bd0fb92b307dd', 'after_sha256': '5d04689f79d11fc6d71e1a5fb52b6df109c94b5fbd49c8505afb4921d1a6759c'}, 'src/dcar_eval/v8/account_classification.py': {'before_sha256': None, 'after_sha256': '65a128de979b72dd952e2ef47f31680dd1ba5b8f9f217b846e4eea847e29dc86'}, 'src/dcar_eval/v8/account_cleanup_runtime.py': {'before_sha256': 'a457ce7c7fe70e400df3e398373fb487251da2d88ce3839957c40d09d94afc7e', 'after_sha256': '6f42486f2b64881501b5db1c131a424284d4982fa73150069c72008005458075'}, 'src/dcar_eval/v8/account_cleanup_snapshot.py': {'before_sha256': '4129c1bf1265ac4fa7a0e295ab5a19e3f80abadb587f6f45978a8ee3e870d732', 'after_sha256': '7828817c71906640a7931f623ef624f1cb0285b15d662235447ed1511c9a2470'}, 'src/dcar_eval/v8/account_directory.py': {'before_sha256': 'e0852e852a67bc17a7710f095969fe3178d1904b820259c6e8f24c163b45627a', 'after_sha256': '8e0c12ddf4c82aadb6bb184343d6d843f39478d1b5713ddbb258684b6b39479b'}, 'src/dcar_eval/v8/account_directory_status.py': {'before_sha256': None, 'after_sha256': '90f07ab7d556a5dad2c7af4aee46e5a6cf5cfd9b5e1b70cdd461a648bc16cb72'}, 'src/dcar_eval/v8/account_operating_status.py': {'before_sha256': 'e68c44488e8ed6de175724916eb2b749b6d8c6dc0add9ec7201abe586971776e', 'after_sha256': 'ca5e8ee2c60541973ff85e46ad50d8f5915736224a3b7ade9f6be3030665f515'}, 'src/dcar_eval/v8/account_roster_capture.py': {'before_sha256': 'e7c2498e84e147ceb07a93cb252a7d321fc2b6312686eb29061f7ef05531634b', 'after_sha256': '410f437aedb2a89e603685ae10f908bfffa664c4bc032a95ebc44bae990e7b75'}, 'src/dcar_eval/v8/api.py': {'before_sha256': 'fdc4c6fde9e0e03d603f15193d798ba7fc63c881c504890fd2d88c2cecaa0a9c', 'after_sha256': '14628d5e20845dfb74a24898d6cd3d8988e3e2c20968970888c5b7b758d9d22b'}, 'src/dcar_eval/v8/billing_reconciliation.py': {'before_sha256': '936d77da3e8ffff245d01d3e0e5e412b6931094c5f0734bc778eed40582ee872', 'after_sha256': '0c77aeb006c6485126bf45e1fb4c532beb33166d076834a06e5843c13200f140'}, 'src/dcar_eval/v8/capture.py': {'before_sha256': '0741b5d83c3a5ffd044e566e9987d9746bcd3cbb36909a53bb909b6829380c4b', 'after_sha256': '097f2193d3485fd8bb1f265e467e368c3095f61561f56ec7414cd43a9df0ec65'}, 'src/dcar_eval/v8/capture_activation_release.py': {'before_sha256': 'f64e22ef1c06b56118939e979915ff55419646bafef74cc6487f6e580763b2b9', 'after_sha256': '9a94b408d942f2685d81f1b8b14782e5b8072f37a34a8b43e39af02ccb61a119'}, 'src/dcar_eval/v8/capture_authorizations.py': {'before_sha256': '7e135dcfedb09d5d63fd2720e43b5ee008fd087f0115077667d5fc9938f9cbe2', 'after_sha256': '9dbb6b63880e0464156ceef77f81b44b258b779de025ee68073707dd942d6510'}, 'src/dcar_eval/v8/capture_availability.py': {'before_sha256': '4a4a7646e5acedad05d38281458560e5afa1989f5a09b72ed514ad0ee6ad21aa', 'after_sha256': 'a55145de18d880b5bbcd80c4e92c2f581627875794f08d7d681d4834e4453067'}, 'src/dcar_eval/v8/capture_batches.py': {'before_sha256': '9329e37a29dfb4fafb2f80acf523d1644e355390e6a94cb1b2735a89402f878a', 'after_sha256': '2104d792fa157efc0f082d513f652ee5bc074772f0338f43806d61cf1673717a'}, 'src/dcar_eval/v8/capture_compensation.py': {'before_sha256': 'b5fa871840683f063bd43574ca13dce969ef3c7c31ec8a593305117a33fe7c5a', 'after_sha256': '0121c8fed929e8c8c5011c1c9da7a7a7f1918ebd7f2dc4a6f9ee0da07c0ec1e6'}, 'src/dcar_eval/v8/capture_integrated_natural_due.py': {'before_sha256': '3e57725ff48a6494753a30de40f34eefa07af505192e1d902030d3b69980bd34', 'after_sha256': '63f63b5e62a1fcbd5614ef722c04998f2aab109ce0f0234f4469350964cb5b5e'}, 'src/dcar_eval/v8/capture_planning.py': {'before_sha256': '8a35c11fcbef7cb611d017e658ceba1ff2ec0c0c29fb7faed66f1032454644cf', 'after_sha256': '5686038dc5063da561834f307f0f3cff7d4a29c6c6a9d4ccadaacd7e824ef853'}, 'src/dcar_eval/v8/capture_quality.py': {'before_sha256': '609d12f541ffe74c2d1ce6e889fdec22279925d48fa9bd28ecb85b8f559ec75d', 'after_sha256': '760b033ae1e7e9ad4324cab729f74aefa88543557718e75b811fdaee6c2d3574'}, 'src/dcar_eval/v8/capture_release.py': {'before_sha256': '32b7e11abbb8d719713bde6e1af74f5d942b8fe56974c4d15084748d10bc7b3a', 'after_sha256': 'fe95cfacb4ae5e2943a3c3fa8575b9354259cf7b0f0e622d6ab8a6f9d4a80513'}, 'src/dcar_eval/v8/capture_release_commands.py': {'before_sha256': 'fb7a5ea34a13ed9fde68f524f4746a59d0570c6181621931a0aefcc6b35891d3', 'after_sha256': '8297c8a53c7d703ca4265752d205f4fa0d17b4d07e4f08049960b9d29d5087b1'}, 'src/dcar_eval/v8/capture_runtime.py': {'before_sha256': 'b1b62538f6819fa1f172728abf6d77206ae0b90dd8b68e9ed282243ea2ce852c', 'after_sha256': '615469f748e8f81842c9f76f39534b86e048f84c434eda02b4cd126255fcd4e2'}, 'src/dcar_eval/v8/capture_singletons.py': {'before_sha256': 'f3181cf202af131cdf162aae6d11b8e32edb13487ad698ede4085b982edb68c1', 'after_sha256': '26b4e840ee1fb99d15e6965ecf9228b1ac73ba3f7b50e7471783001386b35122'}, 'src/dcar_eval/v8/content_scope.py': {'before_sha256': 'ca5313b97e0d92c02083b1d334da8f02740211ad386c3a295ed5837bd726b317', 'after_sha256': '758c84b608407868a616ca8bc7a59f4fe65081396808e31c31fb70e9dedca158'}, 'src/dcar_eval/v8/contracts.py': {'before_sha256': '933327baac24ca2eb46752cbc874b20d68717e6f45c352a04d90c47376c1b9f8', 'after_sha256': '9c66986d6e7267b7beb5c16b36e64df49b1c22d90d05c7d49cfedb1ad08d58db'}, 'src/dcar_eval/v8/durable_runs.py': {'before_sha256': '2b25e1675578baf07fa95f16a2c75b2718bcbd61e4a3f822ec2e101bff9d3feb', 'after_sha256': '8fd03b9e73f766f8449ff99014fb41a14e06bec7c604a5a7e798cbe3756f930e'}, 'src/dcar_eval/v8/evaluation_selectors.py': {'before_sha256': '97629fe924950b00a6ebd9450df519d0958c789461d0a5b2d5b33035fc4f986b', 'after_sha256': '074cfdc6cf8020cd21e3eca816217fb29fa7fc754cd85e3deb5d326352234b41'}, 'src/dcar_eval/v8/metric_observations.py': {'before_sha256': '85614bf422a7243ed8ab20fd1aa1ab2f7f0e37ba4646d99c9ed51175bb429ba1', 'after_sha256': '0c34dfa149f645339bb27f07bb051a595aac46c0f5d184ce0a8aeccec0b72fff'}, 'src/dcar_eval/v8/operations.py': {'before_sha256': '11ae59437b703803b2c7e72d3d334e1fb3cf24f9824442adec42a83fc4f19e21', 'after_sha256': 'd794a80fe4dcfed119ba391ab630feb8fb4fff0ebd7bfac776fae8096d39c673'}, 'src/dcar_eval/v8/paid_drain.py': {'before_sha256': 'ad74385a37c03506b0db11d841f370ca0c2ec0a80194a75a7f65d3244be4f061', 'after_sha256': 'd9ebc760b4df2634a04710d830df587a23489bc067e31029bcd8046495990c3a'}, 'src/dcar_eval/v8/pipeline.py': {'before_sha256': 'ed852aa36d493a07532eb02468bd540a8a15563ff65de4b2c680b69ade3f7262', 'after_sha256': '00d04fed1e41a4a8737abff239582357d01efad2e1d5d365dbf97aac6e45a861'}, 'src/dcar_eval/v8/profile_activations.py': {'before_sha256': '7a042f8b9163a1c8b7f4db7bf73d449f25409b05d37b4701b74c1ca51fcc462c', 'after_sha256': '18cec28c5d37151fde771a3595e9484159864e8a79a2f20a299895c35548823c'}, 'src/dcar_eval/v8/profile_control.py': {'before_sha256': '3839b213718d2eff1d9ef49f6668eaa985fe4ebbae2c5a882af1eb1e84f71dcc', 'after_sha256': 'd1f82feb7d125aa51049c4541d6671d1244c5bbfbb955359d534109f04460ff5'}, 'src/dcar_eval/v8/provider_budget.py': {'before_sha256': '3f09ba595f0a106d2bec3cb1fce8711ada853c4524944fbdcad197a25b409cbc', 'after_sha256': 'c481366d4fd19c1e402ff10d1d26823ef203207d85ca9ee587221f258b6155cc'}, 'src/dcar_eval/v8/report_export.py': {'before_sha256': 'e4a89d5eb9bfa6cfaa8a0d2a1e6e0e5990eb42272c13454126e5a8ff70222cb9', 'after_sha256': '75b07bec17af69138da8335e29a8767c6a330a0a2944172600f4fb9d5fd02564'}, 'src/dcar_eval/v8/report_inputs.py': {'before_sha256': 'acc16e231cabd8a867b9dd46c41937dce9e1ddc09d8d491d4440c8ddb373468a', 'after_sha256': '483a5e4e405717622dcdbde91da37ad315f458d7861ed07ec498931e1a974e69'}, 'src/dcar_eval/v8/reports.py': {'before_sha256': '64de322f386f9c24ff3c10ea65240f01accd1ed3ae63225cdccb26ad4defa2bb', 'after_sha256': '8fa250bc1aa79f32444a5f9cd3e43228464e92e5f4569024c19a8dcfaa4a0237'}, 'src/dcar_eval/v8/runtime_paths.py': {'before_sha256': '85c38df1c0fe6d0f61479f700004c1d5c9363686eb1e48332dc0440e35a84fd1', 'after_sha256': '3f33a5dbdbcfb5b301aa5a3fba372f342952df821117a45f5e2d91ad68ba3173'}, 'src/dcar_eval/v8/schema_v21.py': {'before_sha256': None, 'after_sha256': '4b7efcac61accecd930398f5482f32b7bda310cecc1a31e2771ea138785529ea'}, 'src/dcar_eval/v8/source_routing.py': {'before_sha256': '4a779f10b97c73fe73c2aa96d7343eea0d6cb3ea4b00f9eae727c51e05af9d3d', 'after_sha256': '3f7c516151b28cf4b6ed49372285100550a70fb5bd5e1c804d1622455182ae85'}, 'src/dcar_eval/v8/storage.py': {'before_sha256': 'b46f462d945396eda4f28ddbbcf698c2e248a9746820f6e20e8dac8e29d985eb', 'after_sha256': '8d58dd83b1e05bcf521ce3000dfe6c6c8ead8bc570543d5f02b7513e849b3a14'}, 'tests/test_account_classification_install.py': {'before_sha256': None, 'after_sha256': 'd64942ad016e1667e992ca2ca95cd10899e77383ce436057495a2d81c128f7e8'}, 'tests/test_account_classification_publication.py': {'before_sha256': None, 'after_sha256': 'c7790b73504cba19f87ba2dc67f82e7770b34d074c68e328c7a0be1a79f2da7a'}, 'tests/test_account_classification_release.py': {'before_sha256': None, 'after_sha256': 'a18a3bdccdd177096dc388f46d5340f94926d73e28a9664163b8acbc8adb84a5'}, 'tests/test_account_classification_snapshot_deployment.py': {'before_sha256': None, 'after_sha256': '1051b141c5a4401a86af8c9206ea86c260601209e3ce4a3bb9f31e0f82bfa36d'}, 'tests/test_server_schema_upgrade.py': {'before_sha256': '6a216819d6414725d99a0c567ae7e05a47ff57b2a2eeaeab65e3ecde2d9f9ce5', 'after_sha256': 'eb0b09af07c5e12269bfd1ece9293d90ed89fe7b964bdd67f55c354f4b4b6217'}, 'tests/test_v8_account_classification.py': {'before_sha256': None, 'after_sha256': '46c7232c85abf9935eaa74af41f42c46bfa915d3e686f2cff768193069ee62c1'}, 'tests/test_v8_account_directory_status.py': {'before_sha256': None, 'after_sha256': '73c0becb23c60158d5acf498ae6b40366949acb32a97d808687367c4199bab01'}, 'tests/test_v8_account_status_api.py': {'before_sha256': '18fcaca75ea0a9122f80f4f35d1d9fdea84afe5d6bd30b3df8ea7d96ad8fb88c', 'after_sha256': '03e1d828c959aa9f65f0395c170faef77d5b2ed4ade66b8c939d868d7f41160c'}, 'tests/test_v8_cleanup_account_status.py': {'before_sha256': None, 'after_sha256': 'e27ed71891af75f26df29a78ba1459df6334db72223e98886aa9a37940901b17'}, 'tests/test_v8_evaluation_selectors.py': {'before_sha256': '53aca6fe91a656eb6c4331738a2f44c2233872df6b5d1224ef7bf146af4aa322', 'after_sha256': 'dd1eb74283fb99dbd24d0ada29bf899c553db7552a6ff405448ca35bfe3b4cf5'}, 'tests/test_v8_operations.py': {'before_sha256': 'eb86ab6602d5c29166e1f0074d70f2c3343011178994697a2cf664d675ec48ac', 'after_sha256': '092ddb6e2a3f6512ef30e34e9cc82db1922ebfcadebf7982171f0194d0b30a4b'}, 'tests/test_v8_report_export.py': {'before_sha256': '1c0ba050f3b88080be906c144f11169bbbaaee6539cfd678409c7d32551fe689', 'after_sha256': 'b2ea6a54dcba3973e72e257a2baf57d9af0c64ee635e44c300570c9ef5ce532b'}, 'tests/test_v8_report_inputs.py': {'before_sha256': '599a0899736f593a9050b3ac7680106d8f2ca374630fcaa303d30d725b41626e', 'after_sha256': 'ad26463b7176b40c2383bc1a575ccb36b4d8de418b46e6dae32d96ceb4d40f17'}}
REQUIRED_CHECKS = frozenset({"classification_backend", "classification_reports", "classification_frontend", "classification_release"})
_LOADED_SOURCE = Path(__file__).read_bytes()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError("classification release: " + message)


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def raw(path: Path, *, private: bool = True) -> bytes:
    before = path.lstat()
    require(path.is_absolute() and path.resolve(strict=True) == path and stat.S_ISREG(before.st_mode)
            and before.st_nlink == 1 and before.st_uid == os.geteuid() and not before.st_mode & 0o022
            and (not private or stat.S_IMODE(before.st_mode) == 0o600)
            and 0 <= before.st_size <= 16 * 1024 * 1024, "unsafe receipt or source")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        body = stream.read(16 * 1024 * 1024 + 1)
        opened = os.fstat(stream.fileno())
    def identity(value):
        return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns
    require(identity(before) == identity(opened) == identity(path.lstat()) and len(body) == before.st_size,
            "receipt or source changed during verification")
    return body


def reference(path: Path) -> dict[str, Any]:
    body = raw(path)
    return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body)}


def object_at(ref: Mapping[str, Any]) -> dict[str, Any]:
    body = raw(Path(ref["path"]))
    require(hashlib.sha256(body).hexdigest() == ref.get("sha256")
            and ("byte_size" not in ref or ref["byte_size"] == len(body)), "reference hash or size differs")
    def unique(pairs):
        value = dict(pairs)
        require(len(value) == len(pairs), "duplicate receipt field")
        return value
    def invalid_constant(value):
        raise ValueError("classification release: non-finite receipt value")
    value = json.loads(body, object_pairs_hook=unique, parse_constant=invalid_constant)
    require(isinstance(value, dict), "receipt is not an object")
    return value


def payload_at(ref: Mapping[str, Any], contract: str) -> dict[str, Any]:
    envelope = object_at(ref)
    value = envelope.get("payload")
    require(envelope.get("contract_version") == contract and isinstance(value, dict)
            and envelope.get("payload_sha256") == digest(value), "receipt envelope differs")
    return value


def records(tree: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    require(tree.get("contract") == "writer-source-tree-v1" and isinstance(tree.get("files"), list),
            "source manifest contract differs")
    result = {}
    for record in tree["files"]:
        name = record.get("path")
        require(isinstance(name, str) and bool(name) and not name.startswith("/")
                and not any(char in name for char in ("\0", "\n", "\r", "\\"))
                and all(part not in {"", ".", "..", ".git"} for part in name.split("/"))
                and name not in result, "source manifest path differs")
        require(set(record) == {"path", "sha256", "byte_size", "mode"}
                and isinstance(record["sha256"], str) and len(record["sha256"]) == 64
                and all(c in "0123456789abcdef" for c in record["sha256"])
                and isinstance(record["byte_size"], int) and record["byte_size"] >= 0
                and record["mode"] in {0o644, 0o755}, "source manifest record differs")
        result[name] = dict(record)
    return result


def approved_changes() -> dict[str, dict[str, str | None]]:
    require(bool(REVIEWED_CHANGES) and MODULE not in REVIEWED_CHANGES, "final source review is not frozen")
    result = dict(REVIEWED_CHANGES)
    for name, pair in result.items():
        require(set(pair) == {"before_sha256", "after_sha256"} and pair["before_sha256"] != pair["after_sha256"]
                and all(v is None or (isinstance(v, str) and len(v) == 64 and all(c in "0123456789abcdef" for c in v))
                        for v in pair.values()), "invalid reviewed source hash")
        require(not name.startswith("/") and all(p not in {"", ".", "..", ".git"} for p in name.split("/")),
                "invalid reviewed source path")
    result[MODULE] = {"before_sha256": None, "after_sha256": hashlib.sha256(_LOADED_SOURCE).hexdigest()}
    return dict(sorted(result.items()))


def source_changes(parent: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    left, right = records(parent), records(current)
    require(MODULE not in left and MODULE in right
            and right[MODULE]["sha256"] == hashlib.sha256(_LOADED_SOURCE).hexdigest(),
            "loaded successor is not the reviewed new module")
    changes = {}
    for name in sorted(set(left) | set(right)):
        old, new = left.get(name), right.get(name)
        if old == new:
            continue
        require(old is None or new is None or old["mode"] == new["mode"], "source modes changed")
        old_sha, new_sha = (row["sha256"] if row is not None else None for row in (old, new))
        require(old_sha != new_sha, "source metadata changed without a code delta")
        changes[name] = {"before_sha256": old_sha, "after_sha256": new_sha}
    require(changes == approved_changes(), "source delta differs from the exact reviewed transition")
    return changes


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    """Prove new schema/code while retaining the original capture authority.

    Schema21 removes only obsolete account metadata. The migration receipt is
    separate from the original cleanup install and does not issue paid gates,
    update activation, or claim a new business/transport qualification.
    The bootstrap calls this after verifying every actual source file.
    """
    if build.get("account_profile_recovery_successor") is not None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("verified_account_profile_recovery_release",
            source / "src/dcar_eval/v8/account_profile_recovery_release.py")
        require(spec is not None and spec.loader is not None, "account profile recovery successor verifier is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_inheritance(build=build, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at)
    if build.get("account_profile_successor") is not None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("verified_account_profile_release",
            source / "src/dcar_eval/v8/account_profile_release.py")
        require(spec is not None and spec.loader is not None, "account profile successor verifier is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_inheritance(build=build, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at)
    if build.get("publisher_capacity_successor") is not None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("verified_publisher_capacity_release",
            source / "src/dcar_eval/v8/publisher_capacity_release.py")
        require(spec is not None and spec.loader is not None, "publisher capacity successor verifier is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_inheritance(build=build, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at)
    if build.get("publisher_snapshot_successor") is not None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("verified_publisher_snapshot_release",
            source / "src/dcar_eval/v8/publisher_snapshot_release.py")
        require(spec is not None and spec.loader is not None, "publisher snapshot successor verifier is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_inheritance(build=build, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at)
    if build.get("control_simplification_successor") is not None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("verified_control_simplification_release",
            source / "src/dcar_eval/v8/control_simplification_release.py")
        require(spec is not None and spec.loader is not None, "controls successor verifier is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_inheritance(build=build, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at)
    if build.get("metric_gap_successor") is not None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("verified_metric_gap_release",
            source / "src/dcar_eval/v8/metric_gap_release.py")
        require(spec is not None and spec.loader is not None, "metric gap successor verifier is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_inheritance(build=build, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at)
    if build.get("account_catalog_capture_successor") is not None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("verified_account_catalog_capture_release",
            source / "src/dcar_eval/v8/account_catalog_capture_release.py")
        require(spec is not None and spec.loader is not None, "catalog policy successor verifier is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_inheritance(build=build, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at)
    if build.get("manual_content_scope_successor") is not None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("verified_manual_content_scope_release",
            source / "src/dcar_eval/v8/manual_content_scope_release.py")
        require(spec is not None and spec.loader is not None, "manual code successor verifier is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_inheritance(build=build, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at)
    plan = build.get("account_classification_successor")
    require(isinstance(plan, dict) and plan.get("contract") == CONTRACT
            and plan.get("transition") == TRANSITION, "schema successor contract differs")
    require(plan.get("parent_build", {}).get("sha256") == PARENT_BUILD_SHA256
            and plan.get("parent_install", {}).get("sha256") == PARENT_INSTALL_SHA256,
            "original authority does not match reviewed pins")
    require(dict(build) == payload_at(build_ref, "sealed-build-receipt-v1"), "loaded build differs")
    parent = payload_at(plan["parent_build"], "sealed-build-receipt-v1")
    install = object_at(plan["parent_install"])
    require(parent.get("schema_contract") == {"code_schema": 20, "formal_schema": 20}
            and parent.get("status") == "succeeded" and not parent.get("account_classification_successor")
            and reference(install_path) == plan["parent_install"], "parent authority changed")
    require(install.get("contract") == "account-cleanup-install-v1" and install.get("status") == "installed"
            and install.get("build_receipt") == {k: plan["parent_build"][k] for k in ("path", "sha256")},
            "parent install does not bind original build")
    identity = database.stat()
    require(database.is_absolute() and database.resolve(strict=True) == database
            and install.get("formal_database") == str(database)
            and install.get("installed", {}).get("device") == identity.st_dev
            and install.get("installed", {}).get("inode") == identity.st_ino,
            "formal DB inode changed; migration must be transactional in place")
    require(build.get("schema_contract") == {"code_schema": 21, "formal_schema": 21}
            and build.get("runtime_root_receipt") == parent.get("runtime_root_receipt")
            and build.get("project_root") == parent.get("project_root")
            and build.get("source_root") == str(source) and str(source) != parent.get("source_root")
            and not build.get("postmigration_lineage") and not build.get("cleanup_code_successor"),
            "schema successor runtime binding differs")
    runtime = payload_at(parent["runtime_root_receipt"], "runtime-root-binding-v1")
    require(runtime.get("formal_database") == {"path": str(database), "device": identity.st_dev, "inode": identity.st_ino}
            and runtime.get("project_root") == parent.get("project_root"), "runtime DB binding differs")
    generation, inherited = build.get("account_cleanup_generation", {}), parent.get("account_cleanup_generation", {})
    require(inherited.get("contract") == "account-cleanup-generation-v1"
            and {k: v for k, v in generation.items() if k != "source_tree"}
                == {k: v for k, v in inherited.items() if k != "source_tree"}, "capture authority changed")
    current_tree = object_at(generation["source_tree"])
    old_tree = object_at(inherited["source_tree"])
    require(current_tree.get("source_root") == str(source) and current_tree.get("git") == build.get("git")
            and old_tree.get("source_root") == parent.get("source_root") and old_tree.get("git") == parent.get("git"),
            "source manifest binding differs")
    require(object_at(build["code_successor_plan"]) == {
        "contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1",
        "project_root": build["project_root"], "source_root": str(source), "git": build["git"],
        "source_tree": generation["source_tree"]}, "source plan differs")
    changes = source_changes(old_tree, current_tree)
    require(plan.get("changes") == changes and plan.get("source_tree") == generation["source_tree"],
            "source delta differs from reviewed migration")
    critical = {name: row["sha256"] for name, row in records(current_tree).items()
                if name.startswith(("src/", "config/")) and name.endswith((".py", ".json"))}
    require(build.get("critical_files") == critical, "critical inventory differs")
    migration = object_at(plan["migration"])
    require(migration.get("contract") == "account-classification-install-v1"
            and migration.get("status") == "migrated" and migration.get("from_schema") == 20
            and migration.get("to_schema") == 21 and migration.get("formal_database") == str(database)
            and migration.get("database_identity") == {"device": identity.st_dev, "inode": identity.st_ino}
            and migration.get("authority_build") == plan["parent_build"]
            and migration.get("authority_install") == plan["parent_install"]
            and migration.get("preserved_tables_verified") is True
            and migration.get("paid_gates_issued") == 0
            and migration.get("receipt_sha256") == digest({k: v for k, v in migration.items() if k != "receipt_sha256"}),
            "transactional schema migration proof differs")
    require(plan.get("business_e2e") == "deferred_by_user" and plan.get("transport_qualification") == "not_verified"
            and plan.get("production_rollout") == "approved_by_user"
            and isinstance(plan.get("actor"), str) and bool(plan["actor"].strip())
            and isinstance(plan.get("reason"), str) and bool(plan["reason"].strip()), "review scope differs")
    from datetime import datetime
    issued = datetime.fromisoformat(plan["issued_at"].replace("Z", "+00:00"))
    require(issued.tzinfo is not None and datetime.fromisoformat(migration["migrated_at"].replace("Z", "+00:00")) <= issued,
            "build predates migration")
    if at is not None:
        require(issued <= datetime.fromisoformat(at.replace("Z", "+00:00")), "build is future dated")
    checks = plan.get("checks", {})
    require(set(checks) == REQUIRED_CHECKS, "required checks are incomplete")
    for name, ref in checks.items():
        report = object_at(ref)
        require(report.get("contract") == "account-classification-check-v1" and report.get("name") == name
                and report.get("status") == "passed" and report.get("exit_code") == 0
                and report.get("changes") == changes, "checks do not bind reviewed source")
        require(reference(Path(report["output"]["path"])) == report["output"], "check log changed")
    proof = {"contract": CONTRACT, "transition": TRANSITION, "loaded_build": dict(build_ref),
             "authority_build": plan["parent_build"], "authority_install": plan["parent_install"],
             "authority_runtime": parent["runtime_root_receipt"], "source_tree": generation["source_tree"],
             "migration": plan["migration"], "changes": changes, "checks": checks,
             "issued_at": plan["issued_at"], "actor": plan["actor"], "reason": plan["reason"]}
    proof["proof_sha256"] = digest(proof)
    return {"parent_build": parent, "parent_build_ref": plan["parent_build"], "proof": proof}
