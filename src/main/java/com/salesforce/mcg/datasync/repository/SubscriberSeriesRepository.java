/*****************************************************************************
 * DISCLAIMER:
 * This code is provided "AS IS", without any express or implied warranties,
 * including, but not limited to, the implied warranties of merchantability,
 * fitness for a particular purpose, or non-infringement. Use of this code is
 * at your own risk. In no event shall the authors or copyright holders be
 * liable for any direct, indirect, incidental, special, exemplary, or
 * consequential damages (including, but not limited to, procurement of
 * substitute goods or services; loss of use, data, or profits; or business
 * interruption), however caused and on any theory of liability, whether in
 * contract, strict liability, or tort (including negligence or otherwise)
 * arising in any way out of the use of this code, even if advised of the
 * possibility of such damage.
 ***************************************************************************/

package com.salesforce.mcg.datasync.repository;

import com.salesforce.mcg.datasync.model.SubscriberSeries;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for managing {@link SubscriberSeries} persistence operations.
 *
 * <p>
 * Provides methods for inserting, updating, deleting, and retrieving {@link SubscriberSeries}
 * objects, either singularly or in batches, as well as utility operations on the storage table.
 * </p>
 *
 * @author Rodrigo Costa (rodrigo.costa@salesforce.com)
 * @since 2024-06-09
 */
public interface SubscriberSeriesRepository {

    /**
     * Inserts a new {@link SubscriberSeries} record.
     *
     * @param series the SubscriberSeries instance to insert
     */
    void insert(SubscriberSeries series);

    /**
     * Inserts a batch of {@link SubscriberSeries} records.
     *
     * @param seriesList the list of SubscriberSeries instances to insert
     */
    void insertBatch(List<SubscriberSeries> seriesList);

    /**
     * Updates an existing {@link SubscriberSeries} record.
     *
     * @param series the SubscriberSeries instance to update
     */
    void update(SubscriberSeries series);

    /**
     * Updates a batch of {@link SubscriberSeries} records.
     *
     * @param seriesList the list of SubscriberSeries instances to update
     */
    void updateBatch(List<SubscriberSeries> seriesList);

    /**
     * Inserts or updates a {@link SubscriberSeries} record.
     *
     * @param series the SubscriberSeries instance to upsert
     */
    void upsert(SubscriberSeries series);

    /**
     * Inserts or updates a batch of {@link SubscriberSeries} records.
     *
     * @param seriesList the list of SubscriberSeries instances to upsert
     */
    void upsertBatch(List<SubscriberSeries> seriesList);

    /**
     * Deletes a {@link SubscriberSeries} record.
     *
     * @param series the SubscriberSeries instance to delete
     */
    void delete(SubscriberSeries series);

    /**
     * Deletes a batch of {@link SubscriberSeries} records.
     *
     * @param list the list of SubscriberSeries instances to delete
     */
    void deleteBatch(List<SubscriberSeries> list);

    /**
     * Retrieves a {@link SubscriberSeries} by the subscriber's phone number.
     *
     * @param phoneNumber the phone number to search for
     * @return an Optional containing the matching SubscriberSeries, or empty if not found
     */
    Optional<SubscriberSeries> selectByPhone(long phoneNumber);

    /**
     * Retrieves a list of {@link SubscriberSeries} records by a collection of phone numbers.
     *
     * @param phoneNumbers the collection of phone numbers to search for
     * @return list of matching SubscriberSeries instances
     */
    List<SubscriberSeries> selectByPhones(List<Long> phoneNumbers);

    /**
     * Removes all records from the underlying SubscriberSeries storage.
     */
    void truncateTable();
}

