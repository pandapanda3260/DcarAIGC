"""Exact account/workbench source transition on a verified runtime-v2 parent.

The fixed parent and source pairs are populated only after release review. Empty
pins fail closed. Neither this module nor its plan grants collection permission.
"""
from __future__ import annotations

import hashlib
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Mapping

from . import capture_authorizations as auth, forward_recovery

PLAN = "account-workbench-code-successor-plan-v1"
DECISION = "account-workbench-code-successor-decision-v1"
PROOF = "account-workbench-code-successor-proof-v1"
TRANSITION = "account-workbench-20260907-v1"
PARENT_PLAN = "capture-runtime-successor-plan-v2"
PARENT_SCOPE = "capture_runtime_lease_v1"
# Release-review outputs, never arguments supplied by the CLI or request caller.
PARENT_BUILD_SHA256 = "014d66146252f26396897fd63b7f709d120c09cd82e0899ceec5485710fcd4d1"
SOURCE_TRANSITIONS: dict[str, tuple[str | None, str | None]] = {
    'app/web/app/accounts/AccountsPage.tsx': ('a10b498358995838c1b7d59aed8c93b7c134605dcc51425c3b6b1a4286c6389d', '76db5ddc071212cf5ea385cf0314bea4a48246c82b08a716f79ebb4974dc641c'),
    'app/web/app/accounts/CreateAccountDialog.tsx': (None, '7a85a3f5dab77422233f5340401ae082c5f47a0ed75f64e43cbaacef4b9bee53'),
    'app/web/app/accounts/accounts.module.css': ('e8b4831e486263e31c1573d9bad170fc7d9b1d9732ccdb37cbee92c0b56b9a4f', '2ab101074dfe0882729ee6a98ab196cf807c3468c5099581cc730f427abdd32e'),
    'app/web/app/accounts/douyin-authorization/DouyinAuthorizationPage.tsx': ('177a76b736fbdc55d3962df892c842fa6a3fb1475bfb4cf73fb612e1d1292e14', '958b5bdc95fc707722f20cefd87526132d6fc7134675baf3775b71a4f0b328f9'),
    'app/web/app/components/AccountPageAccess.tsx': (None, 'ff1bc9534204c46452b9d4a4cf2273172241e4c37cf4dd6306de00dd76cee644'),
    'app/web/app/components/AppShell.tsx': ('31be05b7502ee5286a84e64e9ff76552dd2e4e8cc4a12fed6b998696a65764e0', 'b5db3fbd560c2ebe5ae98ec1b2dcba062994121889081d7f86e2ea5977e2e98e'),
    'app/web/app/components/BackToTop.module.css': (None, '56ab78e90c495a096f9ac9c1f442d411e552e3b17c0c85874465cfbd5f10e91c'),
    'app/web/app/components/BackToTop.tsx': (None, '47cb6b40ed4c61f4b500e3b4e421979d0a34156c9ddd4f432fd3f11778b06a6c'),
    'app/web/app/contents/ContentTitle.tsx': (None, '7e932845ef5ca4cf086d778217b9ee2a03ca4f46115e2944741bcbaa6839d5ca'),
    'app/web/app/contents/ContentsPage.module.css': ('b7aab6fc9c44a376dc8ce1c8b391494ba2f6836baecbc2933dbf3a1e9617e223', '5b04625788950276db7513389c5c36c1803a11060a4839b51307e189c3a7f4ca'),
    'app/web/app/contents/ContentsPage.tsx': ('be908a5b125f3df521c75cf3e8d2ee97a6f49e5b746eaf275fa578295dbe4bc0', '1de8115fa9dc7848cb973af3b15fc7914c0047710a95de0be78cf06aea380e3b'),
    'app/web/app/contents/contentForm.ts': ('25665ee9c1b277a045d6855b92e57822b3c284a3b199f88c63ee6396bb2e857e', '9c68ddeb4bb15b14bcf083b152b8e066e69af9b5798ddc11e28176ff36814122'),
    'app/web/app/lib/accountAccess.ts': (None, '14d34da5d87897d2f167c5384d5956191069eefc4adf123a5430bc04911f8852'),
    'app/web/app/lib/api.ts': ('c0739bf95b067ed3186893aa604e5f95655f12eedc64cc982d226156c7c53983', '69693155367642427b10e9531d438f73f47029e94d2054768bca9071d51a26e8'),
    'app/web/app/lib/queries.ts': ('9cb137335309fa45190aef541866c470a8200c7536e441496f6399e94ad00f0e', 'ee0c744dbc67362cdae0ef56f53cbe44657ee810acaf0fe4b6dd532d8ad76658'),
    'app/web/app/lib/queryContracts.ts': ('fa5615330862cc7ac30210b68dc268dc3da0ab86350a8df46c54447cbda5fa85', 'c55ce6dca7f70cdabe495ce17cbe4539a8f5ecbc774a4f02001e4fc96d712f44'),
    'app/web/app/lib/serviceStatus.ts': ('781e86dfc39c2b1d9a1f2549baacf8aa24f7b333409aa6a369184c89970f8008', 'e90d27c963e159e5d9c10f6ba53fb8d279d7a326912272a93e9b905e1cfb982f'),
    'app/web/app/lib/types.ts': ('ca1ec75932fee1c6298c05ada3106f45ec45185e62a134fc711096248d147e26', '1059cc8beb10ebce9a44e41f83ac7413072fdf6f350a9de13cb13c2838de822a'),
    'app/web/app/overview/OverviewPage.tsx': ('68e8148238ef7f7a0129774de35af07b47697b7171165a58d47eb703dbe61106', '0dfddeebc9c534586fa977078e576fda7437d5eaa0b26bded9a50287fd30a35f'),
    'app/web/app/overview/OverviewReport.module.css': (None, '80aff68ea083b05453970bc876dfd630dc2adf7565c90453509279ea1626ffb4'),
    'app/web/app/overview/OverviewReport.tsx': (None, 'c3454aabfcb2bc18457157e302a22f251f63bcac35ad3875e5332bbe644092af'),
    'app/web/app/overview/overviewModel.ts': (None, 'e988cf451570c8b1aa5267b8bb6174e4c0ccd0a16ee652c67f9d2aa281623e81'),
    'app/web/app/users/UsersPage.module.css': (None, '8777df57cd699e72ece853b8ce4bcf54b75fe1157108af33fa0003bccd9e81f7'),
    'app/web/app/users/UsersPage.tsx': ('af3bb37f35f37ade59904d3387b85986af65eb8556aa1e8b882d7ed246af6690', '119c7527b0df60247dc010a6e1cfa1db2ce59d7adce4c77b389c73f207d7b912'),
    'app/web/tests/account-access.test.mjs': (None, 'cd4702d02dd586921306252c2bee27cea35df9d9afed67db39f3d2341caad632'),
    'app/web/tests/account-create.test.mjs': (None, '7b2c3a3c8cb2d5fbe4af0e47a88914110c91d8a95d25054ba43b506e0d3d4895'),
    'app/web/tests/back-to-top.test.mjs': (None, '506693c31687ed24dd5284605d0dbf216d247a0be69641bdce8bb3a85b7e0d66'),
    'app/web/tests/content-title.browser.mjs': (None, '76ff2e0aea8b517c5f26bb9e977b53eb5fdcf544d06af3b767a63bec9028ed82'),
    'app/web/tests/overview-recovery.test.mjs': ('afbcc7f73ac205d936a9211b70152416d7979508d8344cf245e6ea6b5a83aea5', '4c3b3172c911eb5e46ea7de2b3cddb0b8da9daed709fa0383b66338445801f85'),
    'app/web/tests/overview-report.test.mjs': (None, '6558c2bd4f3a61f467706d391f7024a080d861e2ac42158203f2b2538f4214fa'),
    'app/web/tests/page-header.test.mjs': ('b3f3c6e479575f9e63b09c97e694901ec8a72bd843820df3ef8c46fea36aa0a4', '538d9dbc171bfadacd14a6c60d382687519b5851b2838103a6b2d40efe9667e7'),
    'app/web/tests/query-cache.test.mjs': ('7c5b795c3a4bc694d57c9e40c84e3494692777b55fd90c2795ba76eed7cf8ef8', '12aadddc69a18f739c22505bf6e4ddb8dd5d6df0055203cf7da15f61872a06f1'),
    'app/web/tests/rendered-html.test.mjs': ('56a08916e5e524ccad7528b97a87af3704519b48b061d8cd0e96ff651b26102b', '82dc1b641e7496d6dcc5ea83feb08e895baf66d6a25d8016b15e700e04f719b4'),
    'app/web/tests/service-status.test.mjs': ('b58342e5e1c07bda41fcc38c67327bedb8e3e3ad4d3bb71571645cf23ed18b23', '29c3d588e2ef90d726ebac5b295bb2e9a0e143a5975f63846e339565637bc365'),
    'deploy/macos/publish_snapshot.py': ('2eda8acf7554572f2887bd7e438833f25861165d9162ac94672768e531f10d7f', '4ecc0dd9939684b47ef9c761b823b28bf5bb10d4b135aad7d9744f56c8bf7c0d'),
    'docs/v8/account_operating_status.md': ('aae1aebb352a15a5a6f5553faa7a57f63cf03d829cebc191f600253ab4ebdc1e', 'd653577e3f218cce4b74ab4a62b41d651589fe8fd4923208278f2721b9fc7d7a'),
    'scripts/issue_v20_code_successor.py': ('f0619d629087b772e0418d6b0d2f7196b43ac78529562e88c469458735e89479', 'fccd9965419893c6304f386e764be8a478d538f3ce27ae75bfc2c1d2b4ea928a'),
    'scripts/seal_r0_receipts.py': ('8e071ed01581f661853fb27ab730c9feacabd2d7d8905158541802aa6d093fe9', 'a19efaf07994c23977bb55704bdec90a41dcdc5c91df4da6ab9a1541dd74de0d'),
    'src/dcar_eval/dcar_auth/gateway.py': ('1dd155c0d25e5d06ae9f6f0d780ba8cc9c343b48e2f9654a68dcd5dee54156d6', 'd0d522a81589d5f247f7b2d8e0c6d28f2f55632b6bc8080102ba9c8c4b1a8391'),
    'src/dcar_eval/v8/account_creation.py': (None, '2012524182790e8e47a9b0115ecdf9a21de931eb4555bbbe4158f75da768e312'),
    'src/dcar_eval/v8/account_operating_receipts.py': ('6132bc2780a7c54449d476500cf6733b5daf25b4d37fb33d88412e8344b9d719', '9a37fa32da514affa89f29df36842073e5e37975ac1e1ba8786bc7e2c9343b02'),
    'src/dcar_eval/v8/account_operating_status.py': ('3b4ccd8e51e77a03b218f20feb848c7e29c4e47e360db25625c2aeedcdf75cba', 'e68c44488e8ed6de175724916eb2b749b6d8c6dc0add9ec7201abe586971776e'),
    'src/dcar_eval/v8/account_profile_input.py': (None, 'fb5937147a419dd0538ebe4e932bb8edaea3d8c35b0cc231b8121c43b524a523'),
    'src/dcar_eval/v8/account_profile_public.py': (None, '725329fc1339792c848b74e280c5a1ab8b968eda342f368d0265640debe94d3f'),
    'src/dcar_eval/v8/account_roster.py': ('b3f47b90ac580e35efd75d4b4dd576c0a6672a610063cc15e6305f189b957b43', '43d92d724eb60c63d983b6bbfc7898450a81782d43c3d9bbf714338c7868b5e6'),
    'src/dcar_eval/v8/account_roster_capture.py': (None, 'e7c2498e84e147ceb07a93cb252a7d321fc2b6312686eb29061f7ef05531634b'),
    'src/dcar_eval/v8/account_states.py': ('7ad58813983604a3cb1d58dad6c23130f223a9f73c3bd726422462431b8942b9', '225eba4bac918fc3fe19207a326f463f7f70bd8e6c53aefcaf516ba8435c5af5'),
    'src/dcar_eval/v8/api.py': ('df2376d8971f31b6a622236400f5254a61038053b17459b5622b864c44271e93', 'a49aad7a5b5e3267539043343386519ae65534b95c19bab92a15cef251d289a2'),
    'src/dcar_eval/v8/capture.py': ('a516b51215f9e5c8a1db1823b3764ab2648ab7165b05dbd797c6acf2afd3052c', '21166764054b5d734cf9f6017a51a9be2d93f06113f3d5cb9e8b8c8f89d5d9ab'),
    'src/dcar_eval/v8/capture_activation_release.py': ('58b90f41d60c92ea1c7020fa4665dcb1712096f5ef58452ee01ae3235b10ef12', 'f64e22ef1c06b56118939e979915ff55419646bafef74cc6487f6e580763b2b9'),
    'src/dcar_eval/v8/capture_code_successor.py': ('7295076572683fe53c253ed94591976bf9b67b4abe6024e360f7faa6da91e4f3', '3b85febc853c0bef0ccc259cbb0828998f4bacf0d62343c079db5f43f497cc41'),
    'src/dcar_eval/v8/capture_commands.py': ('7fd11525612562cf006ad59579935b7ff68fdf4d6b4afa5b797d23a387211e24', 'f0fc1340660baa085e182a981ae2c8dabdec442ef30878570c379e8ad8b672d4'),
    'src/dcar_eval/v8/capture_integrated_natural_due.py': ('bf25656621ced22760b71744818045df91941c3e859b349b9c1bdaf898937f7e', '3e57725ff48a6494753a30de40f34eefa07af505192e1d902030d3b69980bd34'),
    'src/dcar_eval/v8/capture_operator_release.py': ('2d61ec055f3b8a5ebb8f8d115e415d9020860dac675a0384fd4594720305c237', 'c8f3ed3c1c0e6772387c070723b9dd18d23065562881941ffba3d63350770ee3'),
    'src/dcar_eval/v8/capture_runtime.py': ('a8152734e5f993c16c0d5ad68083144127e643d710710201fceaa906ac3bc643', 'b1b62538f6819fa1f172728abf6d77206ae0b90dd8b68e9ed282243ea2ce852c'),
    'src/dcar_eval/v8/operations.py': ('e3f538fc5b64fbcb5c8a6b954be53615f504c01f34fad76c2c39bd2f7bfbadf6', 'fa02c0855e596dd1172d9a3ed03b38ed06a47cde924076f0cb575ff792616331'),
    'src/dcar_eval/v8/overview_selling_points.py': (None, 'de687c13d32f291ca819e7f7ffef7d7e229760a1eaac9f74681562dffcac3a3c'),
    'src/dcar_eval/v8/pipeline.py': ('7290f25c2a88235a033cc0dd8d6a8099d5bb1774632434c9eab1d5e032840195', 'ed852aa36d493a07532eb02468bd540a8a15563ff65de4b2c680b69ade3f7262'),
    'src/dcar_eval/v8/profile_activations.py': ('f9f64d5ac13c10d1c517f3a1bf2c8f4588f0445fa9445e03c709e55408016c5b', '7a042f8b9163a1c8b7f4db7bf73d449f25409b05d37b4701b74c1ca51fcc462c'),
    'src/dcar_eval/v8/providers.py': ('0bb1abdc45ffb20b63af9b16dd90a02ba8ee55daab88e5f5d3911a34174e0e61', '7ea7f2bbc10eb81ca19a6c26cef7c77e763a8e30f22c0b21632ab0c5181cb66d'),
    'src/dcar_eval/v8/raw_archive.py': ('f2a530e0b564b569e740ce0cb073eb27d768a08d495c640e64f3661fa711e58e', '2c1ae94d745486a157ca589eea8b694b213db83de659445a72b25b3bf9521630'),
    'src/dcar_eval/v8/report_export.py': ('889f078ae342927287a7dd5d20e9bfbb751fa2ee5235cc9df79c8da7d1d0059b', 'e4a89d5eb9bfa6cfaa8a0d2a1e6e0e5990eb42272c13454126e5a8ff70222cb9'),
    'src/dcar_eval/v8/storage.py': ('ac27dfdb929974015da92b79a800077e19da41a7e918ca7a8247dd4ada6f45a4', '0b80ddbbfb0a5d26678d14d90300108ff63fce3b3c3740ab930e7d2eb6954ea9'),
    'src/dcar_eval/v8/system_roster.py': ('48b71cf7a95576e973fda7cd0d6f7f6d33e255b3f9421c7ae41f13db85bbd607', '68077a35774d3155b4cc614f734c5da33a16bd9da52e0f4f1eab2f577bbbec85'),
    'src/dcar_eval/v8/tikhub_scan.py': ('4e9b8cb7b7cb4b5260c88c1a0539abe8a9773685208cb8dc332010808c7c2158', '7a48424d29bbb195d596b5b7127946d1b336694ec6b5c5fe31e905638bd850f9'),
    'src/dcar_eval/v8/transport_natural_due.py': ('0f769c36f793ec5df6db0c158f70af9456e0da42075b1351051bd477d95fedf2', '2243a946d75be16b6ac72c19ea5201a492176290488f822acbf566e54abe16e1'),
    'src/dcar_eval/v8/transport_preparation.py': ('f6a9c4ed9e15c64f21e1aa40b1a3841225cd691adc76f965289a99937a3f3a08', 'a929a23d1bd9057de40a9b3ee873c919ea4c493d415a8da789a89d0943f7ba2d'),
    'tests/capture_v25_fixture.py': (None, '81974cab006697cde6dff7d96a39dbee3e8c7bf4e0a09c3c6b6c9aa27534d163'),
    'tests/test_dcar_auth_account_boundary.py': (None, '1cc0744197983be1da138ac3ad716f32b003540beb5b170a76067ae873ff29f4'),
    'tests/test_dcar_auth_gateway.py': ('31116d32bab0b122c7f34ad6171f84b8f7e395d47f44c6406d84f3c5fccae654', '3d64db42fa077783ab50dc9fd092879a958a9fb74fe599f998bd1f60fa904a9d'),
    'tests/test_issue_v20_code_successor.py': (None, '5e7fa514816ac43eaab9f2127b338943a5006d0b6178b5a7da9a37b009960e33'),
    'tests/test_issue_v20_deployment_receipt.py': (None, '044a85c6d060688884b64b139f9ac828b714b63cda65c8b5ecbe166887bd280a'),
    'tests/test_macos_snapshot_bundle_delta.py': (None, '4c1a1e2e6406124216046d83bf5ed77f9d0206a142a70ff85ebd896c258efb6b'),
    'tests/test_macos_snapshot_publisher.py': ('4f45c4875dd4a81a3c94a7f12cd43dceb84882b822f3dd81c7d870610bdc0301', '4ca51aba873f50da29e22a1aae9b4bf6ee052dbe89c2dec897ba5506bf6e4d07'),
    'tests/test_v20_deferred_acceptance.py': (None, '2339025784d36428e58a365cd205c903ccc3d2f72cf7de719df30c43b068d08c'),
    'tests/test_v20_release_tools.py': (None, 'edef35590c44b5ac0234e02121aa90249cff92864940d0cbb326f98dce49c4e6'),
    'tests/test_v8_account_code_release_integration.py': (None, 'da2a3e7aeba2f11b8348320029208882a91575627d70b7674ad844a869748c89'),
    'tests/test_v8_account_code_successor.py': (None, 'c69fb726c46893d5ae5ec572e459340697f58ef75683b916d0df83844c015c56'),
    'tests/test_v8_account_creation.py': (None, 'f313d1e274ed2226d3d23aa05d6d12dd8c1976db121c37e8120a871ceb3de576'),
    'tests/test_v8_account_creation_api.py': (None, 'e1afec699aba936250a8413cc2446aefa734fb15cb184d9b2955bc8a04c2093d'),
    'tests/test_v8_account_management.py': (None, '2f67aa248c6ffa7dd2937085ae9db45a15ed716bb2a481b2944f873bb941d9bd'),
    'tests/test_v8_account_operating_status.py': ('47bd76ccdcd41251296545ded4e2c0509e3bf41ac43c1cdcd7b971b400ffd5fb', '0a4e9812f10bf48c45fe2bebb9d5488a076d1023bdde9b98641dbdf95f405eff'),
    'tests/test_v8_account_operator_roster.py': (None, '5f0b7422c42b73f2e55f9b8182b365468ebd65619f7bbcd0939679087f5ff684'),
    'tests/test_v8_account_profile_input.py': (None, 'f147ff1cc93a02c40cb8d72a58c01dd9f731efeac8ae722d3cea1b0acd5ddbab'),
    'tests/test_v8_account_profile_public.py': (None, 'd7f3b647987f425fb16839e03bf8c4e3fee23e6bc78ca8de258c7a2d874d49d5'),
    'tests/test_v8_account_roster_capture.py': (None, '8dbb17aace681074f613f720c799e375cf3990693ab2c0c24820972b3ca836f4'),
    'tests/test_v8_account_status_api.py': ('5971bac30144769969e05acc256c07ad93f5bbbd4a13c00e89ee42a4d6b6e304', '18fcaca75ea0a9122f80f4f35d1d9fdea84afe5d6bd30b3df8ea7d96ad8fb88c'),
    'tests/test_v8_api.py': ('109028a7d8a1e0e04ead744575dc09be1d4774bca0fa0d92d1120cfd8280fdb1', '7dc456967db89afbecc8160fdade56c994e298deb2bde45c17f047e9f893ed76'),
    'tests/test_v8_capture_activation_release.py': (None, 'a65675e097e582b860f38ba8309e793cdbedc98ef9bf19ae62700bc7b1aaf289'),
    'tests/test_v8_capture_code_successor.py': (None, '3355c68eb64fa0357a38f64169246af0237a7f179118c04bc43f4a405a5a5cd3'),
    'tests/test_v8_capture_commands.py': (None, '28844737d3a990502d85b6c7034d1a072be85a8df96d2bcdf9a5e8488c43fea7'),
    'tests/test_v8_capture_worker_backoff.py': (None, '61d510476baf42defdb4a22783a1a45772f09ad672f9933b6739c6fd4895cc99'),
    'tests/test_v8_fair_write_lock.py': (None, '489a1b15ca09cdc4d8ff2b09ca027c9a2c477e3610ec04e429f46dc337f339f7'),
    'tests/test_v8_legacy_continuity.py': (None, '91adcf2efcbb09d811a9b598c587d84c96bfa0a0d6901eebdabf353660857e13'),
    'tests/test_v8_native_qualification.py': (None, 'af3aa9e2003e618670ce5400a82f6ef4bb0a4fde5d67c3a60a44b14478a68146'),
    'tests/test_v8_operations.py': ('d1faa107b075dd47de0a4e433840817d23d5b64e8862b98ad5c207fd1ba5fa6d', 'eb86ab6602d5c29166e1f0074d70f2c3343011178994697a2cf664d675ec48ac'),
    'tests/test_v8_overview_selling_points.py': (None, 'bf9d2b8758b3eb2c90769ec6591c4b95f8767db18bdc5bb88b982a1cd6c8cb0e'),
    'tests/test_v8_pipeline.py': ('f97f4cbfe0f56e814925cf66fe9ab6650adc6f113992d01447a1fdb1552f0935', '52a76b5d26a3fcb469dd2b67cd6a8db2f3d2b98d7e280e38ec4acdd472cb107e'),
    'tests/test_v8_system_roster.py': ('cf98f6d2554dc88194686e58013bf21f8c317d77172d1b241f9b458775ce190c', '5a53cb07116581ac5014d61fd53147c5bc2a495484490638a42734ed5cd94c4e'),
    'tests/test_v8_tikhub_reference_case.py': (None, 'ef9db9766fcf00d40e40c414bf5900b66544598bf442c1bc98e668218af7e51c'),
    'tests/test_v8_write_lock.py': (None, 'e0e2ffa262cc5b8cd008f5f539463cba7ff1688d9dd6f09aecd571a8504057db'),
    'tests/test_v8_write_timing.py': (None, 'cb62a3af26639c3c5f4f34b628dd5f2f17efd6f341ced4a5a5892686f2cf592d'),
}
MODULE = "src/dcar_eval/v8/account_code_successor.py"
_LOADED_SOURCE = Path(__file__).read_bytes()
REQUIRED_CHECKS = frozenset({"account_backend", "account_frontend", "account_lint",
    "account_typecheck", "account_ruff", "account_mypy", "account_successor", "account_roster"})


def require(value: bool, message: str) -> None:
    if not value:
        raise auth.AuthorizationError("account_code_successor: " + message)


def _sha(value: bytes | None) -> str | None:
    return hashlib.sha256(value).hexdigest() if value is not None else None


def _digest(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def approved_changes() -> dict[str, dict[str, str | None]]:
    require(_digest(PARENT_BUILD_SHA256) and bool(SOURCE_TRANSITIONS),
            "final installed runtime parent and exact source review are not frozen")
    require(MODULE not in SOURCE_TRANSITIONS, "self source must use the actually loaded bytes")
    result = {}
    for name, pair in SOURCE_TRANSITIONS.items():
        path = Path(name)
        require(not path.is_absolute() and path.as_posix() == name
                and not {"..", ".git", "data", "runtime", "tmp"}.intersection(path.parts)
                and len(pair) == 2 and pair[0] != pair[1]
                and all(value is None or _digest(value) for value in pair), "invalid fixed source transition")
        result[name] = {"before_sha256": pair[0], "after_sha256": pair[1]}
    # There is no fixed-point hash: this module is new in the reviewed parent;
    # its actual loaded bytes must be present in the tested live source archive.
    result[MODULE] = {"before_sha256": None, "after_sha256": _sha(_LOADED_SOURCE)}
    return dict(sorted(result.items()))


def verify_delta(project: Path, parent: Mapping[str, Any], current: Mapping[str, Any], *,
                 live: bool = True) -> dict[str, dict[str, str | None]]:
    require(parent["git"]["head"] == current["git"]["head"], "source HEAD changed")
    left = forward_recovery._successor_archive(parent)
    right = forward_recovery._successor_archive(current, live=live)
    import subprocess
    changes = {}
    for name in sorted(set(left) | set(right)):
        original = None
        if name not in left or name not in right:
            # Match the existing source archive representation: absent entries
            # may be tracked and unchanged at HEAD, never assume they are empty.
            result = subprocess.run(["git", "show", str(current["git"]["head"]) + ":" + name],
                                    cwd=project, check=False, capture_output=True)
            original = result.stdout if result.returncode == 0 else None
        old, new = left.get(name, original), right.get(name, original)
        if old != new:
            changes[name] = {"before_sha256": _sha(old), "after_sha256": _sha(new)}
    require(changes == approved_changes(), "source differs from the exact reviewed account transition")
    old_modes = {row["path"]: row["mode"] for row in parent["git"]["working_tree"]["untracked_files"]}
    new_modes = {row["path"]: row["mode"] for row in current["git"]["working_tree"]["untracked_files"]}
    require(all(new_modes.get(name, 0o644) == old_modes.get(name, 0o644)
                for name in set(old_modes) | set(new_modes)), "source modes changed")
    return changes


def verify_parent(reference: Mapping[str, Any], proof: Mapping[str, Any]) -> None:
    require(reference["sha256"] == PARENT_BUILD_SHA256 and _digest(PARENT_BUILD_SHA256),
            "requested parent is not the frozen installed runtime generation")
    require(proof.get("build_reference") == reference and proof.get("decision_receipt") is not None
            and proof.get("plan_payload", {}).get("contract") == PARENT_PLAN
            and proof["plan_payload"].get("change_scope") == PARENT_SCOPE,
            "parent lacks the verified runtime-v2 decision")
    require(proof.get("contract") == "capture-postaccepted-code-successor-proof-v2"
            and proof.get("proof_sha256") == auth.digest({k: v for k, v in proof.items() if k != "proof_sha256"}),
            "parent proof digest or contract changed")


def verify_plan_fields(plan: Mapping[str, Any], parent: Mapping[str, Any], *, project: Path) -> None:
    require(plan.get("contract") == PLAN and plan.get("transition") == TRANSITION
            and plan.get("project_root") == str(project.resolve())
            and plan.get("required_checks") == sorted(REQUIRED_CHECKS)
            and plan.get("changes") == approved_changes(), "account plan contract or fixed source scope changed")
    verify_parent(plan["installed_parent"], parent)
    old = parent["plan_payload"]
    for key in ("previous_build", "source_deployment", "source_decision_sha256", "active", "release", "operations", "manifest"):
        require(plan.get(key) == old.get(key), "account plan changed the parent " + key)
    require(plan.get("business_e2e") == "deferred_by_user"
            and plan.get("transport_qualification") == "not_verified",
            "code release cannot invent business or statistical acceptance")


def verify_checks(current: Mapping[str, Any], parent: Mapping[str, Any], *, git: Mapping[str, Any]) -> None:
    # The existing six and runtime-v2 logs remain original evidence; fresh full
    # account checks are additional names bound to today's tested source.
    require(current.get("git") == git and current.get("status") == "passed",
            "new account checks do not bind the planned source")
    results, old = current.get("results", {}), parent.get("results", {})
    require(isinstance(results, dict) and isinstance(old, dict)
            and REQUIRED_CHECKS <= set(results), "new account release checks are incomplete")
    require(all(results.get(name) == value for name, value in old.items()), "parent test evidence changed")


def verify_roster_control(connection, *, plan: Mapping[str, Any], deployment: Mapping[str, Any],
                          runtime_bindings: Mapping[str, Any], at: str, portable: bool = False) -> dict[str, Any]:
    """Validate a real post-decision roster change; never use for plan issuance."""
    from .account_roster_capture import validate_code_plan_roster_successor
    from .profile_activations import activation_at
    active = activation_at(connection, at)
    require(active is not None, "current account activation is missing")
    assert active is not None
    decision = deployment["release_decision"]
    return validate_code_plan_roster_successor(connection,
        source_active=plan["active"], source_release=plan["release"], current_active=active,
        source_deployment=deployment, origin_runtime_bindings=decision["runtime_bindings"],
        runtime_bindings=runtime_bindings, manifest=plan["manifest"], operations=plan["operations"],
        at=at, portable=portable)


_RUNTIME: ContextVar[dict[str, Any] | None] = ContextVar("verified_account_runtime_control", default=None)


def _code():
    from . import capture_code_successor
    return capture_code_successor


def control_precheck(plan: Mapping[str, Any], active: Mapping[str, Any], release: Mapping[str, Any]) -> None:
    if active == plan["active"] and release == plan["release"]:
        return
    context = _RUNTIME.get()
    require(context is not None and context["plan"]["active"] == plan["active"]
            and context["plan"]["release"] == plan["release"],
            "activation or RELEASE changed before an issued account successor")
    # This is only structural deferral. The context cannot escape using_plan
    # until its complete source/decision and roster proof have also succeeded.


def control_postcheck(connection, *, plan, deployment, at, portable=False):
    code = _code()
    active, release = code._current_control(connection, at)
    control_precheck(plan, active, release)
    context = _RUNTIME.get()
    if active == plan["active"] and release == plan["release"]:
        return {"active": active, "release": release, "roster_successor": None}
    assert context is not None
    proof = verify_roster_control(connection, plan=plan, deployment=deployment,
        runtime_bindings=context["runtime"], at=at, portable=portable)
    require(proof["target_active"] == active and proof["target_release"] == release,
            "roster proof does not bind the current dispatch control")
    return {"active": active, "release": release, "roster_successor": proof}


def _issued_decision(connection, *, plan, plan_ref, build_ref, runtime_ref, at):
    code = _code()
    row = code._decision_row(connection, build_ref["sha256"])
    require(row is not None, "postseal account decision has not been issued")
    from .transport_receipts import read_transport_receipt
    from .source_routing import parse_time
    receipt = dict(read_transport_receipt(connection, row["id"]))
    expected = code._decision_payload(plan, plan_sha=plan_ref["sha256"], build_sha=build_ref["sha256"],
                                     runtime_sha=runtime_ref["sha256"], at=receipt["recorded_at"])
    require(receipt["payload"] == expected
            and parse_time(plan["issued_at"]) <= parse_time(receipt["recorded_at"]) <= parse_time(at),
            "account decision identity, scope or time changed")
    return receipt


@contextmanager
def runtime_context(connection, *, build, build_ref, at, require_decision=True):
    code = _code()
    sealer, contract = code._tools()
    reference = build.get("code_successor_plan")
    plan = sealer._read_private_json(Path(reference["path"])) if reference else {}
    if plan.get("contract") != PLAN or not require_decision:
        yield
        return
    reference = contract.verified_reference(reference)
    runtime_ref = contract.verified_reference(build["runtime_root_receipt"])
    _issued_decision(connection, plan=plan, plan_ref=reference, build_ref=build_ref, runtime_ref=runtime_ref, at=at)
    token = _RUNTIME.set({"plan": plan, "runtime": {"build_sha256": build_ref["sha256"],
        "runtime_sha256": runtime_ref["sha256"], "config_sha256": plan["origin_runtime_bindings"]["config_sha256"]}})
    try:
        yield
    finally:
        _RUNTIME.reset(token)


@contextmanager
def using_plan(connection, reference, *, project_root: Path, at: str, historical: bool = False):
    code = _code()
    sealer, contract = code._tools()
    plan = sealer._read_private_json(Path(reference["path"]))
    require(not historical, "this account transition cannot itself become another parent")
    require(plan.get("contract") == PLAN and plan.get("transition") == TRANSITION,
            "account plan contract differs")
    require(plan["git"] == sealer._git_record(project_root, allow_working_tree=True), "live source changed after account plan")
    from .source_routing import parse_time
    require(parse_time(plan["issued_at"]) <= parse_time(at), "account plan is future dated")
    active, release = code._current_control(connection, at)
    control_precheck(plan, active, release)
    parent_ref = contract.verified_reference(plan["installed_parent"], project_root=project_root)
    parent = code.current_proof(connection, project_root=project_root,
        build_path=Path(parent_ref["path"]), at=at, _historical=True)
    require(parent is not None, "installed runtime parent lacks its released proof")
    verify_plan_fields(plan, parent, project=project_root)
    previous_ref = contract.verified_reference(plan["previous_build"], project_root=project_root)
    previous = sealer._read_receipt(Path(previous_ref["path"]), contract_version=sealer.SEALED_BUILD_CONTRACT)
    parent_build = sealer._read_receipt(Path(parent_ref["path"]), contract_version=sealer.SEALED_BUILD_CONTRACT)
    source = contract.verified_reference(plan["source_archive"], project_root=project_root)
    require(verify_delta(project_root, parent_build, {"git": plan["git"], "source_archive": source}) == plan["changes"],
            "account source delta changed after review")
    checked = {"project_root": str(project_root.resolve()), "previous_build": previous, "plan": plan,
        "reference": reference, "historical": False, "installed_parent_proof": parent}
    token = code._CHECKED.set(checked)
    try:
        tests_ref = contract.verified_reference(plan["full_checks"], project_root=project_root)
        tests = sealer._read_receipt(Path(tests_ref["path"]), contract_version=sealer.TEST_RESULTS_CONTRACT)
        parent_checks_ref = contract.verified_reference(parent_build["test_results_receipt"], project_root=project_root)
        parent_checks = sealer._read_receipt(Path(parent_checks_ref["path"]), contract_version=sealer.TEST_RESULTS_CONTRACT)
        sealer._verify_test_results_payload(project_root, tests)
        verify_checks(tests, parent_checks, git=plan["git"])
        deployment = contract.validate_deployment_receipt(connection,
            deployment_id=plan["source_deployment"]["deployment_id"], project_root=project_root, require_accepted=True)
        decision = deployment["release_decision"]
        require(deployment["receipt_sha256"] == plan["source_deployment"]["receipt_sha256"]
                and decision["decision_sha256"] == plan["source_decision_sha256"]
                and decision["runtime_bindings"] == plan["origin_runtime_bindings"]
                and decision["operations"] == plan["operations"]
                and decision["transport_manifest"] == plan["manifest"] == forward_recovery._route(),
                "original accepted authority or transport scope changed")
        checked["deployment"] = deployment
        checked["account_control"] = control_postcheck(connection, plan=plan, deployment=deployment, at=at)
        yield checked
    finally:
        code._CHECKED.reset(token)


def prepare_plan(connection, *, project_root: Path, previous_build: Path, evidence_dir: Path,
                 tests: Mapping[str, Path], actor: str, reason: str, at: str):
    from .runtime_database import require_current_process_writer_lock
    require_current_process_writer_lock(connection)
    approved_changes()
    code = _code()
    sealer, _ = code._tools()
    require(bool(actor.strip()) and bool(reason.strip()), "release actor and reason are required")
    parent_ref = code._ref(previous_build)
    installed = code._installed_build_path()
    require(installed is not None and code._ref(installed)["sha256"] == parent_ref["sha256"],
            "requested account parent is not actually installed")
    parent = code.current_proof(connection, project_root=project_root, build_path=previous_build, at=at, _historical=True)
    require(parent is not None, "installed runtime-v2 parent is not released")
    verify_parent(parent_ref, parent)
    active, release = code._current_control(connection, at)
    old = parent["plan_payload"]
    require(active == old["active"] and release == old["release"], "account sealing requires the exact parent control")
    folder = sealer._private_evidence_parent(evidence_dir, project_root)
    sealer._create_evidence_dir(evidence_dir, folder)
    git = sealer._git_record(project_root, allow_working_tree=True)
    archive = evidence_dir / sealer.WORKING_TREE_ARCHIVE
    sealer._write_source_archive(project_root, archive, git)
    source = sealer._source_archive_record(archive, git)
    parent_build = sealer._read_receipt(previous_build, contract_version=sealer.SEALED_BUILD_CONTRACT)
    changes = verify_delta(project_root, parent_build, {"git": git, "source_archive": source})
    checks = sealer._test_results_payload(project_root, paths=tests, git_record=git)
    path = evidence_dir / sealer.TEST_RESULTS_FILENAME
    sealer._write_exclusive(path, sealer._envelope(sealer.TEST_RESULTS_CONTRACT, checks))
    plan = {key: old[key] for key in ("previous_build", "source_deployment", "source_decision_sha256",
                                     "active", "release", "operations", "manifest")}
    plan.update(contract=PLAN, transition=TRANSITION, project_root=str(project_root.resolve()),
        installed_parent=parent_ref, origin_runtime_bindings=parent["origin_runtime_bindings"],
        git=git, source_archive=source, full_checks=code._ref(path), changes=changes,
        required_checks=sorted(REQUIRED_CHECKS), actor=actor, reason=reason, issued_at=at,
        business_e2e="deferred_by_user", transport_qualification="not_verified")
    path = evidence_dir / "code-successor-plan.json"
    sealer._write_exclusive(path, plan)
    reference = code._ref(path)
    with using_plan(connection, reference, project_root=project_root, at=at):
        pass
    return reference


def validate_portable(connection, proof, *, deployment, at):
    """Verify this single new transition using the immutable snapshot ledger."""
    import json
    from .source_routing import parse_time
    from .transport_receipts import _digest as transport_digest
    code = _code()
    require(proof.get("contract") == PROOF
            and proof.get("roster_successor_contract") == "account-roster-code-plan-successor-v1"
            and proof.get("proof_sha256") == auth.digest({k: v for k, v in proof.items() if k != "proof_sha256"}),
            "portable account proof digest changed")
    plan = proof["plan_payload"]
    parent = proof.get("installed_parent_proof")
    require(isinstance(parent, dict), "portable account parent proof is missing")
    verify_plan_fields(plan, parent, project=Path(plan["project_root"]))
    raw = (json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    require(_sha(raw) == proof["plan_reference"]["sha256"]
            and len(raw) == proof["plan_reference"]["byte_size"]
            and proof["build_reference"]["sha256"] == proof["runtime_bindings"]["build_sha256"]
            and proof["runtime_reference"]["sha256"] == proof["runtime_bindings"]["runtime_sha256"],
            "portable account plan or loaded identity changed")
    refs = {"plan": proof["plan_reference"], "previous_build": plan["installed_parent"],
        "source_archive": plan["source_archive"], "full_checks": plan["full_checks"],
        "build": proof["build_reference"], "runtime": proof["runtime_reference"]}
    seen = set()
    for entry in proof["private_references"]:
        role = entry["role"]
        require(role in code.PRIVATE_ROLES and role not in seen
                and type(entry["byte_size"]) is int and entry["byte_size"] > 0
                and all(entry[key] == refs[role][key] for key in ("path", "sha256")), "portable private reference changed")
        seen.add(role)
    require(seen == code.PRIVATE_ROLES, "portable account private reference is missing")
    row = code._decision_row(connection, proof["runtime_bindings"]["build_sha256"])
    require(row is not None, "portable account decision is absent")
    receipt = proof["decision_receipt"]
    require(json.loads(row["details_json"]) == receipt and row["status"] == "succeeded"
            and row["id"] == receipt["receipt_id"]
            and receipt["payload_sha256"] == transport_digest(receipt["payload"])
            and receipt["self_sha256"] == transport_digest({k: v for k, v in receipt.items() if k not in {"self_sha256", "mirror"}}),
            "portable account decision is not the immutable ledger record")
    require(receipt["payload"] == code._decision_payload(plan, plan_sha=proof["plan_reference"]["sha256"],
        build_sha=proof["build_reference"]["sha256"], runtime_sha=proof["runtime_reference"]["sha256"], at=receipt["recorded_at"])
        and parse_time(plan["issued_at"]) <= parse_time(receipt["recorded_at"]) <= parse_time(at),
        "portable account decision scope or time changed")
    decision = deployment["release_decision"]
    require(deployment["status"] == "accepted"
            and proof["source_deployment_sha256"] == deployment["receipt_sha256"]
            and plan["source_deployment"] == {"deployment_id": deployment["deployment_id"], "receipt_sha256": deployment["receipt_sha256"]}
            and plan["source_decision_sha256"] == decision["decision_sha256"]
            and plan["operations"] == decision["operations"]
            and proof["origin_runtime_bindings"] == plan["origin_runtime_bindings"] == decision["runtime_bindings"]
            and proof["manifest"] == plan["manifest"] == decision["transport_manifest"]
            and proof["runtime_bindings"]["config_sha256"] == proof["origin_runtime_bindings"]["config_sha256"],
            "portable account proof changed the original user authority")
    token = _RUNTIME.set({"plan": plan, "runtime": proof["runtime_bindings"]})
    try:
        code.validate_portable(connection, parent, deployment=deployment, at=at)
        control = control_postcheck(connection, plan=plan, deployment=deployment, at=at, portable=True)
        require(proof["active"] == control["active"]
                and proof["release_event_id"] == control["release"]["id"]
                and proof["release_event_hash"] == control["release"]["event_hash"]
                and proof.get("roster_successor") == control["roster_successor"],
                "portable account control or roster successor changed")
        from .capture_activation_release import validate_installed_activation_successor
        from .profile_activations import activation_at
        validate_installed_activation_successor(connection, source_deployment=deployment,
            current_active=activation_at(connection, at), runtime_bindings=proof["origin_runtime_bindings"],
            manifest=proof["manifest"], at=at, portable=True)
        return dict(proof)
    finally:
        _RUNTIME.reset(token)
